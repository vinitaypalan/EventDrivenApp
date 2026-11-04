exports.handler = async (event) => {
  const { orderId, itemId } = event.detail ?? {};

  console.info("OrderCreated event received", {
    orderId,
    itemId,
    eventId: event.id,
  });
};
