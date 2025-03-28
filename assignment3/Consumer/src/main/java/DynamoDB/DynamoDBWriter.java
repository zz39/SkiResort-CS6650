package DynamoDB;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.*;

public class DynamoDBWriter {

  private static final String TABLE_NAME = "LiftRides";
  private static final Logger logger = LoggerFactory.getLogger(DynamoDBWriter.class);
  private static final DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
      .region(Region.US_EAST_1)
      .build();

  public static void saveBatchToDynamoDB(List<String> messages) {
    try {
      List<WriteRequest> writeRequests = new ArrayList<>();
      for (String message : messages) {
        JSONObject json = new JSONObject(message);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("resortID", AttributeValue.builder().s(json.getString("resortID")).build());
        item.put("seasonID", AttributeValue.builder().s(json.getString("seasonID")).build());
        item.put("dayID", AttributeValue.builder().s(json.getString("dayID")).build());
        item.put("skierID", AttributeValue.builder().s(json.getString("skierID")).build());
        item.put("body", AttributeValue.builder().s(message).build());

        writeRequests.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());
      }

      BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
          .requestItems(Collections.singletonMap(TABLE_NAME, writeRequests))
          .build();

      dynamoDbClient.batchWriteItem(batchRequest);
      logger.info("Batch write successful for {} items", messages.size());

    } catch (Exception e) {
      logger.error("Batch write failed: {}", e.getMessage());
    }
  }
}
