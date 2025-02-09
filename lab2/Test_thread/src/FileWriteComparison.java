import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class FileWriteComparison {
  private static final int NUM_THREADS = 500;
  private static final int STRINGS_PER_THREAD = 2000;
  private static final String BASE_FILENAME = "output";
  private static final ConcurrentLinkedQueue<String> sharedQueue = new ConcurrentLinkedQueue<>();

  public static void main(String[] args) throws Exception {
    System.out.println("Starting file write comparison test...");

    System.out.println("\nTesting Approach 1 (immediate writing):");
    testApproach1();

    System.out.println("\nTesting Approach 2 (batch writing):");
    testApproach2();

    System.out.println("\nTesting Approach 3 (shared collection):");
    testApproach3();
  }

  // Approach 1: Immediate writing
  private static void testApproach1() throws Exception {
    long startTime = System.currentTimeMillis();

    // Create the file writer
    FileWriter fw = new FileWriter(BASE_FILENAME + "_approach1.txt");
    BufferedWriter writer = new BufferedWriter(fw);
    PrintWriter synchronizedWriter = new PrintWriter(writer);

    Thread[] threads = new Thread[NUM_THREADS];

    // Create and start all threads
    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i] = new Thread(() -> {
        try {
          for (int j = 0; j < STRINGS_PER_THREAD; j++) {
            String line = System.currentTimeMillis() + "," +
                Thread.currentThread().getId() + "," +
                j;
            synchronized(synchronizedWriter) {
              synchronizedWriter.println(line);
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      threads[i].start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    synchronizedWriter.close();
    long endTime = System.currentTimeMillis();
    System.out.printf("Approach 1 - Time taken: %d ms%n", (endTime - startTime));
  }


  // Approach 2: Batch writing per thread
  private static void testApproach2() throws Exception {
    long startTime = System.currentTimeMillis();

    FileWriter fw = new FileWriter(BASE_FILENAME + "_approach2.txt");
    BufferedWriter writer = new BufferedWriter(fw);
    PrintWriter synchronizedWriter = new PrintWriter(writer);

    Thread[] threads = new Thread[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i] = new Thread(() -> {
        try {
          // Create a StringBuilder to store all strings for this thread
          StringBuilder batch = new StringBuilder();
          for (int j = 0; j < STRINGS_PER_THREAD; j++) {
            batch.append(System.currentTimeMillis())
                .append(",")
                .append(Thread.currentThread().getId())
                .append(",")
                .append(j)
                .append("\n");
          }
          // Write all strings at once
          synchronized(synchronizedWriter) {
            synchronizedWriter.write(batch.toString());
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      threads[i].start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    synchronizedWriter.close();
    long endTime = System.currentTimeMillis();
    System.out.printf("Approach 2 - Time taken: %d ms%n", (endTime - startTime));
  }

  // Approach 3: Shared collection
  private static void testApproach3() throws Exception {
    long startTime = System.currentTimeMillis();

    // Clear the queue from any previous test
    sharedQueue.clear();

    Thread[] threads = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < STRINGS_PER_THREAD; j++) {
          String line = System.currentTimeMillis() + "," +
              Thread.currentThread().getId() + "," +
              j;
          sharedQueue.offer(line);
        }
      });
      threads[i].start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // Write all strings from the shared collection
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(
        new FileWriter(BASE_FILENAME + "_approach3.txt")))) {
      for (String line : sharedQueue) {
        writer.println(line);
      }
    }

    long endTime = System.currentTimeMillis();
    System.out.printf("Approach 3 - Time taken: %d ms%n", (endTime - startTime));
  }
}