package Consumer;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;

public class SkierConsumer {
  private static final String QUEUE_NAME = "SkierQueue";
  private static ConcurrentHashMap<Integer, List<String>> skierData = new ConcurrentHashMap<>();
  private static final int THREAD_COUNT = 50;

  public static void main(String[] args) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("13.216.71.149");  // Update with RMQ EC2 IP
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare(QUEUE_NAME, true, false, false, null);

    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      executor.execute(() -> {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        processMessage(message);
        try {
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    };

    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
  }

  private static void processMessage(String message) {
    // Parse JSON and update skierData HashMap
    System.out.println("Processed: " + message);
  }
}