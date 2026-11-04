const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, PutCommand } = require("@aws-sdk/lib-dynamodb");
const { EventBridgeClient, PutEventsCommand } = require("@aws-sdk/client-eventbridge");

const dynamoDb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const eventBridge = new EventBridgeClient({});

exports.handler = async (event) => {
  let order;
  try {
    order = typeof event.body === "string"
      ? JSON.parse(event.body)
      : event.body ?? event;
  } catch {
    return response(400, { message: "Request body must be valid JSON" });
  }

  if (
    !order ||
    typeof order !== "object" ||
    Array.isArray(order) ||
    typeof order.orderId !== "string" ||
    !order.orderId ||
    typeof order.itemId !== "string" ||
    !order.itemId
  ) {
    return response(400, { message: "orderId and itemId are required strings" });
  }

  await dynamoDb.send(
    new PutCommand({
      TableName: process.env.ORDERS_TABLE,
      Item: order,
    }),
  );

  try {
    const result = await eventBridge.send(
      new PutEventsCommand({
        Entries: [
          {
            EventBusName: process.env.ORDERS_EVENT_BUS,
            Source: "orders.service",
            DetailType: "OrderCreated",
            Detail: JSON.stringify({
              orderId: order.orderId,
              itemId: order.itemId,
              order,
            }),
          },
        ],
      }),
    );

    if (result.FailedEntryCount > 0) {
      const failures = (result.Entries ?? [])
        .filter((entry) => entry.ErrorCode)
        .map(({ ErrorCode, ErrorMessage }) => ({ ErrorCode, ErrorMessage }));
      console.error("Failed to publish OrderCreated event", {
        orderId: order.orderId,
        itemId: order.itemId,
        failures,
      });
      return response(500, {
        message: "Order was saved, but its event could not be published",
        orderId: order.orderId,
        itemId: order.itemId,
      });
    }
  } catch (error) {
    console.error("Failed to publish OrderCreated event", {
      orderId: order.orderId,
      itemId: order.itemId,
      error: error.message,
    });
    return response(500, {
      message: "Order was saved, but its event could not be published",
      orderId: order.orderId,
      itemId: order.itemId,
    });
  }

  return response(201, {
    message: "Order item saved",
    orderId: order.orderId,
    itemId: order.itemId,
  });
};

function response(statusCode, body) {
  return {
    statusCode,
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  };
}
