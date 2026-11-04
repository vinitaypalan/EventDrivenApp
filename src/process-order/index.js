exports.handler = async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records ?? []) {
    try {
      const eventBridgeEvent = JSON.parse(record.body);
      const { source, "detail-type": detailType, detail } = eventBridgeEvent;

      if (source !== "orders.service" || detailType !== "OrderCreated") {
        throw new Error("SQS message is not an orders.service OrderCreated event");
      }

      if (
        !detail ||
        typeof detail.orderId !== "string" ||
        !detail.orderId ||
        typeof detail.itemId !== "string" ||
        !detail.itemId
      ) {
        throw new Error("OrderCreated event is missing orderId or itemId");
      }

      console.info("Processing order", {
        orderId: detail.orderId,
        itemId: detail.itemId,
        order: detail.order,
        eventId: eventBridgeEvent.id,
        messageId: record.messageId,
      });

      // Add order-processing business logic here without coupling it to order creation.
    } catch (error) {
      console.error("Failed to process order message", {
        messageId: record.messageId,
        error: error.message,
      });
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  return { batchItemFailures };
};
