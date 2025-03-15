package Consumer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

public class LiftRideConsumer {
  private final static String QUEUE_NAME = "SkierQueue";
  private static Map<Integer, List<LiftRide>> skierLiftRides = new ConcurrentHashMap<>();
  private static Gson gson = new Gson();

  private static ConnectionFactory factory = new ConnectionFactory();
  private static final int CHANNEL_POOL_SIZE = 120;
  private static final int NUM_OF_THREAD = 1;
  private static final BlockingQueue<Channel> channelPool = new LinkedBlockingQueue<>(CHANNEL_POOL_SIZE);

  static { initializeChannelPool();}

  public static void main(String[] argv) {
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    for (int i = 0; i < NUM_OF_THREAD; i++) {
      new Thread(new ConsumerThread(channelPool, QUEUE_NAME, skierLiftRides, gson)).start();
    }
  }

  private static void initializeChannelPool() {
    factory.setHost("34.220.147.139");
    try {
      Connection connection = factory.newConnection();
      for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channelPool.offer(channel);
      }
    } catch (IOException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public static int getSkierIdFromMessage(String message) {
    JsonObject jsonObject = gson.fromJson(message, JsonObject.class);
    return jsonObject.get("skierID").getAsInt();
  }

  public static LiftRide getLiftRideFromMessage(String message) {
    JsonObject jsonObject = gson.fromJson(message, JsonObject.class);
    return gson.fromJson(jsonObject.get("body"), LiftRide.class);
  }
}