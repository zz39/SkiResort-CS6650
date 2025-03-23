package Client;

import io.swagger.client.model.LiftRide;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultithreadedClient {

  private static int TOTAL_REQUESTS = 50000;
  // Threads Configurations:
  private static int ORIGINAL_THREADS = 32;
  private static int REQUESTS_PER_THREAD = 100;

  public static void main(String[] args) {
    System.out.println("Starting client...");

    // Create a single shared BlockingDeque
    BlockingDeque<LiftRide> sharedQueue = new LinkedBlockingDeque<>(TOTAL_REQUESTS);

    // Create and start event generator with shared queue
    SkierEventGenerator eventGenerator = new SkierEventGenerator(TOTAL_REQUESTS, sharedQueue);
    Thread skiEventThread = new Thread(eventGenerator);
    skiEventThread.start();
    System.out.println("SkierEventGenerator thread started");

    // Brief delay to allow event generator to start populating queue
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    long startTime = System.currentTimeMillis();

    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        ORIGINAL_THREADS,
        ORIGINAL_THREADS,
        5000L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(REQUESTS_PER_THREAD * ORIGINAL_THREADS),
        new ThreadPoolExecutor.CallerRunsPolicy()
    );
    System.out.println("Thread pool executor created");

    CountDownLatch countDownLatch = new CountDownLatch(TOTAL_REQUESTS);
    AtomicInteger successfulRequests = new AtomicInteger(0);
    AtomicInteger failedRequests = new AtomicInteger(0);

    System.out.println("Creating " + (TOTAL_REQUESTS / REQUESTS_PER_THREAD) + " tasks");

    // Use the shared queue for all PostingSkiInfo tasks
    for (int i = 0; i < TOTAL_REQUESTS / REQUESTS_PER_THREAD; i++) {
      executor.execute(new PostingSkiInfo(
          sharedQueue,  // Use the shared queue
          REQUESTS_PER_THREAD,
          successfulRequests,
          failedRequests,
          countDownLatch
      ));
      System.out.println("Task " + (i + 1) + " submitted");
    }

    System.out.println("Waiting for all requests to complete...");
    try {
      // Wait for event generator to finish
      skiEventThread.join();
      System.out.println("Event generator finished");

      // Wait for all requests to complete
      countDownLatch.await();
      System.out.println("All requests completed");
    } catch (InterruptedException e) {
      System.out.println("Main thread got interrupted");
      executor.shutdownNow();
      Thread.currentThread().interrupt();
      return;
    }

    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;

    List<Long> latencies = PostingSkiInfo.getLatencies();
    List<String> requestLogs = PostingSkiInfo.getRequestLogs();

    System.out.println("Successful requests: " + successfulRequests.get());
    System.out.println("Failed requests: " + failedRequests.get());
    System.out.println("Collected " + latencies.size() + " latency measurements");

    computeAndDisplayStats(latencies, totalTime);
    writeToCSV(requestLogs);

    executor.shutdown();
    System.out.println("Client shutdown complete");
  }

  private static void computeAndDisplayStats(List<Long> latencies, long totalTime) {
    Collections.sort(latencies);

    long min = latencies.get(0);
    long max = latencies.get(latencies.size() - 1);
    long sum = latencies.stream().mapToLong(Long::longValue).sum();
    double mean = sum / (double) latencies.size();
    double median = latencies.size() % 2 == 0 ?
        (latencies.get(latencies.size()/2 - 1) + latencies.get(latencies.size()/2)) / 2.0 :
        latencies.get(latencies.size()/2);
    long p99 = latencies.get((int) (latencies.size() * 0.99));

    double throughput = TOTAL_REQUESTS / (totalTime / 1000.0);

    System.out.println("Statistics:");
    System.out.println("Mean response time: " + mean + " ms");
    System.out.println("Median response time: " + median + " ms");
    System.out.println("99th percentile response time: " + p99 + " ms");
    System.out.println("Min response time: " + min + " ms");
    System.out.println("Max response time: " + max + " ms");
    System.out.println("Throughput: " + throughput + " requests/second");
  }

  private static void writeToCSV(List<String> requestLogs) {
    try (FileWriter writer = new FileWriter("latencies.csv")) {
      writer.write("Start Time,Request Type,Latency,Response Code\n");
      for (String log : requestLogs) {
        writer.write(log + "\n");
      }
      System.out.println("Latency data saved to latencies.csv");
    } catch (IOException e) {
      System.err.println("Error writing CSV file: " + e.getMessage());
    }
  }
}