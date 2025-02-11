package Client;

import io.swagger.client.model.LiftRide;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultithreadedClient {
  // client configurations
  private static int TOTAL_REQUESTS = 200000;
  private static int ORIGINAL_THREADS = 32;
  private static int REQUESTS_PER_THREAD = 1000;
  // multithreaded data structure
  private static BlockingDeque<LiftRide> generatorQueue = new LinkedBlockingDeque<>(TOTAL_REQUESTS);
  private static AtomicInteger successfulRequests = new AtomicInteger(0);
  private static AtomicInteger failedRequests = new AtomicInteger(0);

  public static void main(String[] args) {
    // Start LiftRide generation
    Thread skiEventThread = new Thread(new SkierEventGenerator(TOTAL_REQUESTS, generatorQueue));
    skiEventThread.start();

    long startTime = System.currentTimeMillis();

    // Create ThreadPoolExecutor
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        ORIGINAL_THREADS,
        ORIGINAL_THREADS,
        5000L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(REQUESTS_PER_THREAD * ORIGINAL_THREADS),
        new ThreadPoolExecutor.CallerRunsPolicy()
    );

    CountDownLatch countDownLatch = new CountDownLatch(TOTAL_REQUESTS);

    for (int i = 0; i < TOTAL_REQUESTS / REQUESTS_PER_THREAD; i++) {
      executor.execute(new PostingSkiInfo(generatorQueue, REQUESTS_PER_THREAD, successfulRequests, failedRequests, countDownLatch));
    }

    // wait until all threads to finish
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      System.out.println("Main thread got interrupted");
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    // print results
    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;
    System.out.println("Successful requests: " + successfulRequests.get());
    System.out.println("Failed requests: " + failedRequests.get());
    System.out.println("Total time spent: " + totalTime + " ms");
    System.out.println("Throughput: " + (TOTAL_REQUESTS / (totalTime / 1000.0)) + " requests/second");
    executor.shutdown();
  }

}
