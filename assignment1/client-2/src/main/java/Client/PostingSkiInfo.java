package Client;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class PostingSkiInfo implements Runnable {
  private BlockingQueue<LiftRide> queue;
  private int numRequests;
  private AtomicInteger successfulRequests;
  private AtomicInteger failedRequests;
  private CountDownLatch count;
  private Random random = new Random();
  private final int MAX_RETRIES = 5;
  private final int RESORT_ID_RANGE = 10;
  private final int SKIER_ID_RANGE = 100000;
  private final String SEASON_ID = "2025";
  private final String DAY_ID = "1";

  // Shared list to store latencies
  private static final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
  private static final List<String> requestLogs = Collections.synchronizedList(new ArrayList<>());

  public PostingSkiInfo(BlockingQueue<LiftRide> queue, int numRequests,
      AtomicInteger successfulRequests, AtomicInteger failedRequests, CountDownLatch count) {
    this.queue = queue;
    this.numRequests = numRequests;
    this.successfulRequests = successfulRequests;
    this.failedRequests = failedRequests;
    this.count = count;
  }

  @Override
  public void run() {
    System.out.println("Starting PostingSkiInfo thread");  // Debug log 10
    SkiersApi skiersApi = createApiClient();

    for (int i = 0; i < numRequests; i++) {
      LiftRide liftRide = queue.poll();
      if (liftRide == null) {
        System.out.println("No lift ride available in queue");  // Debug log 11
        break;
      }

      System.out.println("Processing request " + (i + 1));  // Debug log 12
      boolean success = false;
      int retries = 0;

      while (!success && retries < MAX_RETRIES) {
        long startTime = System.currentTimeMillis();
        try {
          ApiResponse<Void> apiResponse = skiersApi.writeNewLiftRideWithHttpInfo(
              liftRide, random.nextInt(RESORT_ID_RANGE) + 1, SEASON_ID, DAY_ID, random.nextInt(SKIER_ID_RANGE) + 1
          );
          System.out.println("Request completed with status: " + apiResponse.getStatusCode());  // Debug log 13

          long endTime = System.currentTimeMillis();
          long latency = endTime - startTime;

          // Record latency and response code
          latencies.add(latency);
          requestLogs.add(startTime + ",POST," + latency + "," + apiResponse.getStatusCode());

          if (apiResponse.getStatusCode() == 201) {
            successfulRequests.incrementAndGet();
            success = true;
          } else if (apiResponse.getStatusCode() == 400) {
            retries++;
          }
        } catch (ApiException e) {
          System.err.println("API Exception: " + e.getMessage());  // Debug log 14

          long endTime = System.currentTimeMillis();
          long latency = endTime - startTime;
          latencies.add(latency);
          requestLogs.add(startTime + ",POST," + latency + ",ERROR");

          System.err.println("Exception: " + liftRide);
          e.printStackTrace();

        } finally {
          count.countDown();
        }
      }
      if (!success) {
        failedRequests.incrementAndGet();
      }
    }
  }

  private SkiersApi createApiClient() {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath("http://3.84.49.179:8080/Assignment1/");
    return new SkiersApi(apiClient);
  }

  public static List<Long> getLatencies() {
    return latencies;
  }

  public static List<String> getRequestLogs() {
    return requestLogs;

  }

}