package Consumer;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class SkierConsumer {
  private static final String QUEUE_NAME = "SkierQueue";
  private static final int BATCH_SIZE = 100; // Process messages in batches of 50
  private static final int THREAD_COUNT = 100; // Adjust based on CPU cores
  private static final ConcurrentHashMap<Integer, List<String>> skierData = new ConcurrentHashMap<>();

  public static void main(String[] args) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("13.216.71.149");  // Update with RabbitMQ EC2 IP
    factory.setAutomaticRecoveryEnabled(true);
    factory.setNetworkRecoveryInterval(5000);
    factory.setRequestedHeartbeat(30);

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare(QUEUE_NAME, true, false, false, null);

    // **Set Prefetch**: Prevent overloading a single consumer
    channel.basicQos(BATCH_SIZE);

    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    List<Delivery> batchMessages = new ArrayList<>();

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      synchronized (batchMessages) {
        batchMessages.add(delivery);

        if (batchMessages.size() >= BATCH_SIZE) {
          List<Delivery> batchToProcess = new ArrayList<>(batchMessages);
          batchMessages.clear(); // Clear buffer

          executor.execute(() -> processBatch(batchToProcess, channel));
        }
      }
    };

    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
    System.out.println(" [*] Waiting for messages...");
  }

  private static void processBatch(List<Delivery> batch, Channel channel) {
    try {
      System.out.println("Processing batch of size: " + batch.size());

      for (Delivery delivery : batch) {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        processMessage(message);
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); // Acknowledge each message
      }

    } catch (Exception e) {
      System.err.println("Error processing batch: " + e.getMessage());

      // Optionally requeue messages in case of failure
      batch.forEach(delivery -> {
        try {
          channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      });
    }
  }

  private static void processMessage(String message) {
    // Parse JSON and update skierData HashMap
    System.out.println("Processed: " + message);
  }
}