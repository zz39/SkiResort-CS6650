package Client;

import io.swagger.client.model.LiftRide;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class MultiThreadLiftRideClient {
    private static int TOTAL_REQUESTS = 200000;
    private static int INITIAL_THREADS = 100;
    private static int REQUESTS_PER_THREAD = 2000;

    private static BlockingQueue<LiftRide> GeneratorQueue = new LinkedBlockingQueue<>(TOTAL_REQUESTS);
    private static AtomicInteger successfulRequests = new AtomicInteger(0);
    private static AtomicInteger failedRequests = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
      // Start LiftRide generation
      Thread eventGeneratorThread = new Thread(new SkierEventGenerator(GeneratorQueue, TOTAL_REQUESTS));
      eventGeneratorThread.start();

      long startTime = System.currentTimeMillis();

      // Create ThreadPoolExecutor
      ThreadPoolExecutor executor = new ThreadPoolExecutor(
          INITIAL_THREADS,
          INITIAL_THREADS,
          5000L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(REQUESTS_PER_THREAD * INITIAL_THREADS),
          new ThreadPoolExecutor.CallerRunsPolicy()
      );

      CountDownLatch completed = new CountDownLatch(TOTAL_REQUESTS);

      for (int i = 0; i < TOTAL_REQUESTS / REQUESTS_PER_THREAD; i++) {
        executor.execute(new PostingSkiInfo(GeneratorQueue, REQUESTS_PER_THREAD, successfulRequests, failedRequests, completed));
      }

      // wait until all threads to finish
      try{
        completed.await();
      } catch (InterruptedException e) {
        System.out.println("Main thread was interrupted");
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }

      // Print results
      long totalTime = System.currentTimeMillis() - startTime;
      System.out.println("Successful requests: " + successfulRequests.get());
      System.out.println("Failed requests: " + failedRequests.get());
      System.out.println("Total time: " + totalTime + " ms");
      System.out.println("Throughput: " + (TOTAL_REQUESTS / (totalTime / 1000.0)) + " requests/second");
      executor.shutdown();
    }
}