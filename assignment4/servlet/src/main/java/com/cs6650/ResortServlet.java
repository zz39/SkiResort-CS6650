package com.cs6650;
// AWS SDK imports
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.util.*;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;
import java.io.IOException;
import org.json.simple.JSONObject;

import java.io.File;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

@WebServlet("/resorts/*")
public class ResortServlet extends HttpServlet {

  // DynamoDB client - Use the same credentials handling as in SkierServlet
  private static final DynamoDbClient dynamoDbClient;
  private static final String TABLE_NAME = "SkiersCounters";

  static {
    // Try to initialize DynamoDB client with explicit profile credentials if available
    try {
      // For AWS lab environment with temporary credentials
      AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(
          "ASIAQXNPG4AF2IJL7TGB",
          "aDOtZaetBrtQ9Gp3kDq8GcNjHX6Vpmk0WZmBp30P",
          "IQoJb3JpZ2luX2VjEOf//////////wEaCXVzLXdlc3QtMiJHMEUCIQCrO52iXfumsHZeA4y/QZJ092DXXSRhTpiY1CHLgSNjmAIgULvDS5WOum0mW1Z0lEPGYpU3qjGlNMg9znt0HgwlxfsqpgIIcBAAGgwwNTAyOTY0NDY5ODciDB+Nde6HXm8X/48oWCqDAr4V49c3f0OOAfEJlNv8zt4ItZIfOZ6rRbsT0GLhjKd/r9uRMHnhIK6/rsQfQZ3to/CIOs/ZszLfx6iTVGAULd1uCUpMFz5FYenOXGN3goZkhCXph11X/D7tm8mIh2h9pumi4hDbVjkQpLEHTK63w9doiD7Ft2la8e1XZXpU7VASWotGX/bMJVICJ3LVI7fWVAVp60iE5v1oozhDBnqMTLH1/1XEVFPcH+zEVafA0mIgkYlPLDQY+/OBxBcFutGIH9KkwL0b2NBs4dRplzcBj2dKyV97tL6ZAGVPEjOjfxR9DzBbMnGoGu5LFbeB9J7W1VoBh2LoCFr15aaxvHvRU30/tJ8wvO2HwAY6nQHiAuuBmT4bkKNvMgbkm7GOyUIFOGa1zBm+ctuhyUwDWldbINyxjS0FxWostP4xxlZHSWmbpe1MmwjZNr1ST+qkmj1UbaY5By+4Z1dq8keqccy7nWn0WXdwDZ8g8ehxGmv2VzF7IS8srqbVFU24zyeUHTH3ViYo9fgl61Ot0LDCFGDCpkzQJ1Pl+XKc0H9iq+eWfX/3RmF7raUF4Zfd"
      );

      dynamoDbClient = DynamoDbClient.builder()
          .region(Region.US_EAST_1)
          .credentialsProvider(StaticCredentialsProvider.create(sessionCredentials))
          .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize DynamoDB client: " + e.getMessage(), e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();
    System.out.println("GET request to ResortServlet: " + urlPath);

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("{\"message\": \"missing parameters\"}");
      return;
    }

    String[] urlParts = urlPath.split("/");

    try {
      // Check if path matches /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
      if (urlParts.length == 7 && urlParts[2].equals("seasons") &&
          urlParts[4].equals("day") && urlParts[6].equals("skiers")) {

        String resortID = urlParts[1];
        String seasonID = urlParts[3];
        String dayID = urlParts[5];

        handleResortDaySkiers(resortID, seasonID, dayID, res);
      } else {
        res.setStatus(HttpServletResponse.SC_NOT_FOUND);
        res.getWriter().write("{\"message\": \"invalid url pattern for resort\"}");
      }
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      res.getWriter().write("{\"message\": \"" + e.getMessage() + "\"}");
      e.printStackTrace();
    }
  }

  /**
   * Handle GET /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
   * Returns the number of unique skiers at resort/season/day
   */
  private void handleResortDaySkiers(String resortID, String seasonID, String dayID, HttpServletResponse res) throws IOException {
    try {
      // Construct the key for the uniqueSkiers counter
      String counterKey = resortID + "_" + seasonID + "_" + dayID;

      // Query for the counter directly
      GetItemRequest getRequest = GetItemRequest.builder()
          .tableName("SkiersCounters")
          .key(Map.of(
              "CounterId", AttributeValue.builder().s("UniqueSkiers").build(),
              "Key", AttributeValue.builder().s(counterKey).build()
          ))
          .build();

      GetItemResponse getResponse = dynamoDbClient.getItem(getRequest);

      int numSkiers = 0;
      if (getResponse.hasItem() && getResponse.item().containsKey("Count")) {
        numSkiers = Integer.parseInt(getResponse.item().get("Count").n());
      }

      // Build response JSON
      JSONObject responseJson = new JSONObject();
      responseJson.put("resortID", resortID);
      responseJson.put("seasonID", seasonID);
      responseJson.put("dayID", dayID);
      responseJson.put("numSkiers", numSkiers);

      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(responseJson.toJSONString());
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      res.getWriter().write("{\"message\": \"Error querying DynamoDB: " + e.getMessage() + "\"}");
      e.printStackTrace();
    }
  }
}