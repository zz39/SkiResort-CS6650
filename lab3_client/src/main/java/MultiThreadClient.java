import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MultiThreadClient {
  private static final String url = "http://localhost:8080/lab3/hello";
  private static final int NUM_THREADS = 100;

  static class RequestThread extends Thread {
    private final HttpClient client;

    public RequestThread() {
      this.client = new HttpClient();
    }

    @Override
    public void run() {
      GetMethod method = new GetMethod(url);
      method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
          new DefaultHttpMethodRetryHandler(3, false));

      try {
        int statusCode = client.executeMethod(method);

        if (statusCode != HttpStatus.SC_OK) {
          System.err.println("Method failed: " + method.getStatusLine());
        }

        byte[] responseBody = method.getResponseBody();
        System.out.println("Thread " + Thread.currentThread().getId() +
            " received: " + new String(responseBody));

      } catch (HttpException e) {
        System.err.println("Fatal protocol violation: " + e.getMessage());
        e.printStackTrace();
      } catch (IOException e) {
        System.err.println("Fatal transport error: " + e.getMessage());
        e.printStackTrace();
      } finally {
        method.releaseConnection();
      }
    }
  }

  public static void main(String[] args) {
    List<Thread> threads = new ArrayList<>();

    // Record start time
    long startTime = System.currentTimeMillis();

    // Create and start threads
    for (int i = 0; i < NUM_THREADS; i++) {
      Thread thread = new RequestThread();
      threads.add(thread);
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        System.err.println("Thread interrupted: " + e.getMessage());
      }
    }

    // Calculate and print execution time
    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;

    System.out.println("\nExecution completed:");
    System.out.println("Total time taken: " + totalTime + " milliseconds");
    System.out.println("Average time per request: " + (totalTime / NUM_THREADS) + " milliseconds");
  }
}