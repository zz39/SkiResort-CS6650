package Client;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import java.util.Random;
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
  final private int MAX_RETRIES = 5;
  final private int RESORT_ID_RANGE = 10;
  final private int SKIER_ID_RANGE = 100000;
  final private String SEASON_ID = "2024";
  final private String DAY_ID = "1";

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
    SkiersApi skiersApi = createApiClient();
    for (int i = 0; i < numRequests; i++) {
      LiftRide liftRide = queue.poll();
      if (liftRide == null) {
        break;
      }
      boolean success = false;
      int retries = 0;
      while (!success && retries < MAX_RETRIES) {
        try {
          ApiResponse<Void> apiResponse = skiersApi.writeNewLiftRideWithHttpInfo(liftRide, random.nextInt(RESORT_ID_RANGE) + 1, SEASON_ID,DAY_ID, random.nextInt(SKIER_ID_RANGE) + 1);
          if (apiResponse.getStatusCode() == 201) {
            successfulRequests.incrementAndGet();
            success = true;
          } else if (apiResponse.getStatusCode() >= 400) {
            retries++;
          }
        } catch (ApiException e) {
          System.err.println("Exception when calling SkiersApi#writeNewLiftRide for " + liftRide);
          e.printStackTrace();
        } finally {
          this.count.countDown();
        }
      }
      if (!success) {
        failedRequests.incrementAndGet();
      }
    }
  }

  private SkiersApi createApiClient() {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath("http://34.225.55.133:8080/Assignment2/");
    return new SkiersApi(apiClient);
  }
}