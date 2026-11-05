const { createHash, randomUUID } = require("node:crypto");
const { isDeepStrictEqual } = require("node:util");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  TransactWriteCommand,
  UpdateCommand,
} = require("@aws-sdk/lib-dynamodb");
const { EventBridgeClient, PutEventsCommand } = require("@aws-sdk/client-eventbridge");

const dynamoDb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const eventBridge = new EventBridgeClient({});
const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const LEASE_DURATION_MS = 60_000;
const IDEMPOTENCY_TTL_SECONDS = 30 * 24 * 60 * 60;

exports.createHandler = createHandler;
exports.handler = createHandler({ dynamoDb, eventBridge });

function createHandler(clients) {
  return async (event) => {
    const batchItemFailures = [];

    for (const record of event.Records ?? []) {
      let orderId;

      try {
        const body = JSON.parse(record.body);

        if (isLegacyOrderCreatedEvent(body)) {
          const detail = body.detail;
          if (
            !detail ||
            typeof detail.orderId !== "string" ||
            !detail.orderId ||
            typeof detail.itemId !== "string" ||
            !detail.itemId
          ) {
            throw new Error("Legacy OrderCreated event is missing orderId or itemId");
          }
          orderId = detail.orderId;
          console.info("Order message processed", {
            messageId: record.messageId,
            orderId,
            status: "LEGACY_EVENT_ACKNOWLEDGED",
          });
          continue;
        }

        const order = body;
        orderId = order?.orderId;
        validateOrder(order);

        const idempotencyKey =
          record.messageAttributes?.IdempotencyKey?.stringValue ??
          record.messageAttributes?.IdempotencyKey?.StringValue;
        if (typeof idempotencyKey !== "string" || !IDEMPOTENCY_KEY_PATTERN.test(idempotencyKey)) {
          throw new Error("Missing or invalid Idempotency-Key message attribute");
        }

        const status = await processOrder(order, idempotencyKey, record.messageId, clients);
        console.info("Order message processed", {
          messageId: record.messageId,
          orderId,
          status,
        });
      } catch (error) {
        console.error("Order message processing failed", {
          messageId: record.messageId,
          orderId,
          status: "FAILED",
          errorName: error.name,
        });
        batchItemFailures.push({ itemIdentifier: record.messageId });
      }
    }

    return { batchItemFailures };
  };
}

function isLegacyOrderCreatedEvent(value) {
  return value?.source === "orders.service" && value["detail-type"] === "OrderCreated";
}

function validateOrder(order) {
  if (
    !order ||
    typeof order !== "object" ||
    Array.isArray(order) ||
    typeof order.orderId !== "string" ||
    !order.orderId ||
    typeof order.itemId !== "string" ||
    !order.itemId
  ) {
    throw new Error("Order must contain non-empty orderId and itemId strings");
  }
}

async function processOrder(order, idempotencyKey, messageId, clients) {
  const claim = await claimIdempotencyKey(idempotencyKey, order, clients);
  if (claim.completed) {
    return "DUPLICATE_COMPLETED";
  }

  const orderItem = { ...order, idempotencyKey };
  let orderAlreadyPersisted = false;

  try {
    await clients.dynamoDb.send(
      new TransactWriteCommand({
        TransactItems: [
          {
            Put: {
              TableName: process.env.ORDERS_TABLE,
              Item: orderItem,
              ConditionExpression: "attribute_not_exists(orderId) AND attribute_not_exists(itemId)",
            },
          },
          {
            Update: {
              TableName: process.env.IDEMPOTENCY_TABLE,
              Key: { idempotencyKey },
              UpdateExpression: "SET orderPersisted = :persisted, updatedAt = :updatedAt",
              ConditionExpression:
                "#status = :inProgress AND processingToken = :token AND leaseExpiry = :leaseExpiry",
              ExpressionAttributeNames: { "#status": "status" },
              ExpressionAttributeValues: {
                ":persisted": true,
                ":updatedAt": new Date().toISOString(),
                ":inProgress": "IN_PROGRESS",
                ":token": claim.processingToken,
                ":leaseExpiry": claim.leaseExpiry,
              },
            },
          },
        ],
      }),
    );
  } catch (error) {
    if (error.name !== "TransactionCanceledException") {
      throw error;
    }

    const existing = await getOrder(order.orderId, order.itemId, clients);
    if (!existing || !matchesOrder(existing, order)) {
      throw error;
    }

    if (existing.idempotencyKey !== idempotencyKey) {
      await completeIdempotencyKey(idempotencyKey, claim, clients);
      return "DUPLICATE_ORDER";
    }

    orderAlreadyPersisted = true;
  }

  if (!(await stillOwnClaim(idempotencyKey, claim, clients))) {
    throw new Error("Idempotency lease is no longer owned by this invocation");
  }

  const existingOrder = orderAlreadyPersisted
    ? await getOrder(order.orderId, order.itemId, clients)
    : orderItem;
  if (existingOrder?.idempotencyKey === idempotencyKey) {
    await publishOrderProcessed(order, idempotencyKey, messageId, clients);
  }

  await completeIdempotencyKey(idempotencyKey, claim, clients);
  return orderAlreadyPersisted ? "RECOVERED_PERSISTED_ORDER" : "COMPLETED";
}

async function claimIdempotencyKey(idempotencyKey, order, clients) {
  const { orderId } = order;
  const requestHash = hashOrder(order);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const now = Date.now();
    const nowIso = new Date(now).toISOString();
    const leaseExpiry = now + LEASE_DURATION_MS;
    const processingToken = randomUUID();
    const expiresAt = Math.floor(now / 1000) + IDEMPOTENCY_TTL_SECONDS;

    try {
      await clients.dynamoDb.send(
        new PutCommand({
          TableName: process.env.IDEMPOTENCY_TABLE,
          Item: {
            idempotencyKey,
            orderId,
            requestHash,
            status: "IN_PROGRESS",
            createdAt: nowIso,
            updatedAt: nowIso,
            leaseExpiry,
            expiresAt,
            processingToken,
          },
          ConditionExpression: "attribute_not_exists(idempotencyKey)",
        }),
      );
      return { processingToken, leaseExpiry };
    } catch (error) {
      if (error.name !== "ConditionalCheckFailedException") {
        throw error;
      }
    }

    const existing = await getIdempotencyRecord(idempotencyKey, clients);
    if (!existing) {
      continue;
    }
    if (existing.orderId !== orderId) {
      throw new Error("Idempotency key is already associated with a different order");
    }
    if (existing.requestHash && existing.requestHash !== requestHash) {
      throw new Error("Idempotency key is already associated with a different request");
    }
    if (!existing.requestHash && existing.status === "COMPLETED") {
      const persistedOrder = await getOrder(order.orderId, order.itemId, clients);
      if (
        !persistedOrder ||
        persistedOrder.idempotencyKey !== idempotencyKey ||
        !matchesOrder(persistedOrder, order)
      ) {
        throw new Error("Idempotency key is associated with an unverifiable request");
      }
    }
    if (existing.status === "COMPLETED") {
      return { completed: true };
    }
    if (existing.status !== "IN_PROGRESS") {
      throw new Error("Idempotency record has an unsupported status");
    }

    const recoveryNow = Date.now();
    const recoveryNowIso = new Date(recoveryNow).toISOString();
    const recoveryLeaseExpiry = recoveryNow + LEASE_DURATION_MS;
    const recoveryProcessingToken = randomUUID();

    try {
      await clients.dynamoDb.send(
        new UpdateCommand({
          TableName: process.env.IDEMPOTENCY_TABLE,
          Key: { idempotencyKey },
          UpdateExpression:
            "SET processingToken = :token, leaseExpiry = :leaseExpiry, updatedAt = :updatedAt, expiresAt = :expiresAt, requestHash = :requestHash",
          ConditionExpression:
            "#status = :inProgress AND leaseExpiry <= :now AND orderId = :orderId",
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: {
            ":token": recoveryProcessingToken,
            ":leaseExpiry": recoveryLeaseExpiry,
            ":updatedAt": recoveryNowIso,
            ":expiresAt": Math.floor(recoveryNow / 1000) + IDEMPOTENCY_TTL_SECONDS,
            ":requestHash": requestHash,
            ":inProgress": "IN_PROGRESS",
            ":now": recoveryNow,
            ":orderId": orderId,
          },
        }),
      );
      return { processingToken: recoveryProcessingToken, leaseExpiry: recoveryLeaseExpiry };
    } catch (error) {
      if (error.name !== "ConditionalCheckFailedException") {
        throw error;
      }
    }
  }

  const latest = await getIdempotencyRecord(idempotencyKey, clients);
  if (latest?.status === "COMPLETED") {
    return { completed: true };
  }
  throw new Error("Another invocation currently owns the idempotency lease");
}

function hashOrder(order) {
  const canonicalOrder = JSON.stringify(canonicalize(order));
  return createHash("sha256").update(canonicalOrder).digest("hex");
}

function canonicalize(value) {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalize(value[key])]),
    );
  }
  return value;
}

async function getIdempotencyRecord(idempotencyKey, clients) {
  const result = await clients.dynamoDb.send(
    new GetCommand({
      TableName: process.env.IDEMPOTENCY_TABLE,
      Key: { idempotencyKey },
      ConsistentRead: true,
    }),
  );
  return result.Item;
}

async function getOrder(orderId, itemId, clients) {
  const result = await clients.dynamoDb.send(
    new GetCommand({
      TableName: process.env.ORDERS_TABLE,
      Key: { orderId, itemId },
      ConsistentRead: true,
    }),
  );
  return result.Item;
}

function matchesOrder(existing, incoming) {
  return Object.entries(incoming).every(([key, value]) => isDeepStrictEqual(existing[key], value));
}

async function stillOwnClaim(idempotencyKey, claim, clients) {
  const existing = await getIdempotencyRecord(idempotencyKey, clients);
  return (
    existing?.status === "IN_PROGRESS" &&
    existing.processingToken === claim.processingToken &&
    existing.leaseExpiry === claim.leaseExpiry
  );
}

async function publishOrderProcessed(order, idempotencyKey, messageId, clients) {
  const endpointEvent = {
    Source: "orders.processing",
    DetailType: "OrderProcessed",
    Detail: JSON.stringify({
      orderId: order.orderId,
      itemId: order.itemId,
      idempotencyKey,
      messageId,
    }),
  };

  const result = await clients.eventBridge.send(
    new PutEventsCommand({
      Entries: [
        process.env.ORDERS_EVENT_ENDPOINT
          ? { EndpointId: process.env.ORDERS_EVENT_ENDPOINT, ...endpointEvent }
          : { EventBusName: process.env.ORDERS_EVENT_BUS, ...endpointEvent },
      ],
    }),
  );

  if (result.FailedEntryCount > 0) {
    throw new Error("EventBridge rejected the order-processed event");
  }
}

async function completeIdempotencyKey(idempotencyKey, claim, clients) {
  const now = Date.now();
  await clients.dynamoDb.send(
    new UpdateCommand({
      TableName: process.env.IDEMPOTENCY_TABLE,
      Key: { idempotencyKey },
      UpdateExpression: "SET #status = :completed, updatedAt = :updatedAt, leaseExpiry = :leaseExpiry",
      ConditionExpression:
        "#status = :inProgress AND processingToken = :token AND leaseExpiry = :claimedLeaseExpiry",
      ExpressionAttributeNames: { "#status": "status" },
      ExpressionAttributeValues: {
        ":completed": "COMPLETED",
        ":updatedAt": new Date(now).toISOString(),
        ":leaseExpiry": now,
        ":inProgress": "IN_PROGRESS",
        ":token": claim.processingToken,
        ":claimedLeaseExpiry": claim.leaseExpiry,
      },
    }),
  );
}
