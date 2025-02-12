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
    System.out.println("Starting PostingSkiInfo thread");
    SkiersApi skiersApi = createApiClient();

    for (int i = 0; i < numRequests; i++) {
      LiftRide liftRide;
      try {
        liftRide = queue.take();  // Blocking until an item is available
      } catch (InterruptedException e) {
        System.out.println("Thread interrupted while waiting for queue");
        Thread.currentThread().interrupt();
        return;
      }

      System.out.println("Processing request " + (i + 1));
      boolean success = false;
      int retries = 0;

      while (!success && retries < MAX_RETRIES) {
        long startTime = System.currentTimeMillis();
        try {
          ApiResponse<Void> apiResponse = skiersApi.writeNewLiftRideWithHttpInfo(
              liftRide, random.nextInt(RESORT_ID_RANGE) + 1, SEASON_ID, DAY_ID, random.nextInt(SKIER_ID_RANGE) + 1
          );

          long endTime = System.currentTimeMillis();
          long latency = endTime - startTime;

          latencies.add(latency);
          requestLogs.add(startTime + ",POST," + latency + "," + apiResponse.getStatusCode());

          if (apiResponse.getStatusCode() == 201) {
            successfulRequests.incrementAndGet();
            success = true;
          } else {
            retries++;
          }
        } catch (ApiException e) {
          System.err.println("API Exception: " + e.getMessage());

          long endTime = System.currentTimeMillis();
          long latency = endTime - startTime;
          latencies.add(latency);
          requestLogs.add(startTime + ",POST," + latency + ",ERROR");

          retries++;
        }
      }

      if (success) {
        count.countDown();
      } else {
        failedRequests.incrementAndGet();
        count.countDown();
      }
    }
  }

  private SkiersApi createApiClient() {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath("http://107.23.18.223:8080/Assignment1/");
    return new SkiersApi(apiClient);
  }

  public static List<Long> getLatencies() {
    return latencies;
  }

  public static List<String> getRequestLogs() {
    return requestLogs;

  }

}