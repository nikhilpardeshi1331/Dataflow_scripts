/**
 * Transformation function to convert Avro record to JSON object.
 * @param {Object} record - Avro record object.
 * @return {string} - JSON string representation of the transformed record.
 */
function transformAvroRecord(record) {
  console.log("Received record:", JSON.stringify(record));

  var transformedRecord = {
    Customer_id: record.CustomerID ? record.CustomerID.toString() : '',
    date: record.OrderDate ? record.OrderDate.toString() : '',
    time: record.OrderTimestamp ? record.OrderTimestamp.toString() : '',
    order_id: record.OrderID ? record.OrderID.toString() : '',
    items: record.Item ? record.Item.toString() : '',
    amount: record.Quantity ? parseInt(record.Quantity) : 0,
    mode: record.PaymentMethod ? record.PaymentMethod.toString() : '',
    restaurant: record.Restaurant ? record.Restaurant.toString() : '',
    status: record.Status ? record.Status.toString() : '',
    ratings: record.Rating ? parseInt(record.Rating) : 0,
    feedback: record.Feedback ? record.Feedback.toString() : ''
  };

  console.log("Transformed record:", JSON.stringify(transformedRecord));

  return JSON.stringify(transformedRecord);
}
