const assert = require("node:assert/strict");
const { test } = require("node:test");
const { createHandler } = require("./index");

process.env.ORDERS_TABLE = "Orders";
process.env.IDEMPOTENCY_TABLE = "EventDrivenAppIdempotencyTable";
process.env.ORDERS_EVENT_BUS = "OrdersEventBus";

class FakeDynamoDb {
  constructor() {
    this.idempotencyRecords = new Map();
    this.orders = new Map();
    this.transactionCount = 0;
  }

  async send(command) {
    const { input } = command;

    if (command.constructor.name === "PutCommand") {
      if (this.idempotencyRecords.has(input.Item.idempotencyKey)) {
        throw namedError("ConditionalCheckFailedException");
      }
      this.idempotencyRecords.set(input.Item.idempotencyKey, { ...input.Item });
      return {};
    }

    if (command.constructor.name === "GetCommand") {
      const item =
        input.TableName === process.env.IDEMPOTENCY_TABLE
          ? this.idempotencyRecords.get(input.Key.idempotencyKey)
          : this.orders.get(orderKey(input.Key.orderId, input.Key.itemId));
      return { Item: item && { ...item } };
    }

    if (command.constructor.name === "UpdateCommand") {
      const item = this.idempotencyRecords.get(input.Key.idempotencyKey);
      if (!item) {
        throw namedError("ConditionalCheckFailedException");
      }

      const values = input.ExpressionAttributeValues;
      if (values[":completed"]) {
        if (
          item.status !== values[":inProgress"] ||
          item.processingToken !== values[":token"] ||
          item.leaseExpiry !== values[":claimedLeaseExpiry"]
        ) {
          throw namedError("ConditionalCheckFailedException");
        }
        item.status = values[":completed"];
        item.updatedAt = values[":updatedAt"];
        item.leaseExpiry = values[":leaseExpiry"];
      } else {
        if (
          item.status !== values[":inProgress"] ||
          item.leaseExpiry > values[":now"] ||
          item.orderId !== values[":orderId"]
        ) {
          throw namedError("ConditionalCheckFailedException");
        }
        item.processingToken = values[":token"];
        item.leaseExpiry = values[":leaseExpiry"];
        item.updatedAt = values[":updatedAt"];
        item.expiresAt = values[":expiresAt"];
        item.requestHash = values[":requestHash"];
      }
      return {};
    }

    if (command.constructor.name === "TransactWriteCommand") {
      this.transactionCount += 1;
      const put = input.TransactItems[0].Put;
      const update = input.TransactItems[1].Update;
      const key = orderKey(put.Item.orderId, put.Item.itemId);
      const claim = this.idempotencyRecords.get(update.Key.idempotencyKey);
      if (
        this.orders.has(key) ||
        claim?.status !== update.ExpressionAttributeValues[":inProgress"] ||
        claim.processingToken !== update.ExpressionAttributeValues[":token"] ||
        claim.leaseExpiry !== update.ExpressionAttributeValues[":leaseExpiry"]
      ) {
        throw namedError("TransactionCanceledException");
      }
      this.orders.set(key, { ...put.Item });
      claim.orderPersisted = true;
      claim.updatedAt = update.ExpressionAttributeValues[":updatedAt"];
      return {};
    }

    throw new Error(`Unexpected DynamoDB command ${command.constructor.name}`);
  }
}

function namedError(name) {
  const error = new Error(name);
  error.name = name;
  return error;
}

function orderKey(orderId, itemId) {
  return `${orderId}:${itemId}`;
}

function message(orderId, idempotencyKey, messageId = orderId) {
  return {
    messageId,
    body: JSON.stringify({ orderId, itemId: "item-1", quantity: 1 }),
    messageAttributes: idempotencyKey
      ? { IdempotencyKey: { stringValue: idempotencyKey, dataType: "String" } }
      : {},
  };
}

function createTestHandler(dynamoDb) {
  let publishedEvents = 0;
  const handler = createHandler({
    dynamoDb,
    eventBridge: {
      async send() {
        publishedEvents += 1;
        return { FailedEntryCount: 0 };
      },
    },
  });
  return { handler, getPublishedEvents: () => publishedEvents };
}

test("persists once, completes the key, and acknowledges a repeated delivery", async () => {
  const dynamoDb = new FakeDynamoDb();
  const { handler, getPublishedEvents } = createTestHandler(dynamoDb);
  const record = message("order-1", "request-1");

  assert.deepEqual(await handler({ Records: [record] }), { batchItemFailures: [] });
  assert.equal(dynamoDb.orders.size, 1);
  assert.equal(dynamoDb.idempotencyRecords.get("request-1").status, "COMPLETED");
  assert.equal(getPublishedEvents(), 1);

  assert.deepEqual(await handler({ Records: [record] }), { batchItemFailures: [] });
  assert.equal(dynamoDb.transactionCount, 1);
  assert.equal(getPublishedEvents(), 1);
});

test("rejects reuse of an idempotency key with a different request", async () => {
  const dynamoDb = new FakeDynamoDb();
  const { handler, getPublishedEvents } = createTestHandler(dynamoDb);
  const record = message("order-1", "request-1");

  assert.deepEqual(await handler({ Records: [record] }), { batchItemFailures: [] });
  const conflictingRecord = {
    ...record,
    body: JSON.stringify({ orderId: "order-1", itemId: "item-2", quantity: 1 }),
  };

  assert.deepEqual(await handler({ Records: [conflictingRecord] }), {
    batchItemFailures: [{ itemIdentifier: record.messageId }],
  });
  assert.equal(dynamoDb.transactionCount, 1);
  assert.equal(getPublishedEvents(), 1);
});

test("recovers an expired lease and returns only failed message IDs", async () => {
  const dynamoDb = new FakeDynamoDb();
  dynamoDb.idempotencyRecords.set("request-recovery", {
    idempotencyKey: "request-recovery",
    orderId: "order-recovery",
    status: "IN_PROGRESS",
    createdAt: new Date(Date.now() - 120_000).toISOString(),
    updatedAt: new Date(Date.now() - 120_000).toISOString(),
    leaseExpiry: Date.now() - 60_000,
    expiresAt: Math.floor(Date.now() / 1000) + 60,
    processingToken: "expired-token",
  });
  const { handler } = createTestHandler(dynamoDb);

  const result = await handler({
    Records: [
      message("order-recovery", "request-recovery", "recovered"),
      message("order-invalid", undefined, "missing-key"),
    ],
  });

  assert.deepEqual(result, { batchItemFailures: [{ itemIdentifier: "missing-key" }] });
  assert.equal(dynamoDb.idempotencyRecords.get("request-recovery").status, "COMPLETED");
  assert.equal(dynamoDb.orders.size, 1);
});
