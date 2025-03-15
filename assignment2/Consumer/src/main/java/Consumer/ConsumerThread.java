package Consumer;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class ConsumerThread implements Runnable{
  private BlockingQueue<Channel> channelPool;
  private String queueName;
  private Map<Integer, List<LiftRide>> skierLiftRides;
  private Gson gson;

  public ConsumerThread(BlockingQueue<Channel> channelPool, String queueName,
      Map<Integer, List<LiftRide>> skierLiftRides, Gson gson) {
    this.channelPool = channelPool;
    this.queueName = queueName;
    this.skierLiftRides = skierLiftRides;
    this.gson = gson;
  }

  @Override
  public void run() {
    Channel channel = null;
    try {
      channel = channelPool.poll();

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        System.out.println(" [x] Received '" + message + "'");

        try {
          LiftRide liftRide = LiftRideConsumer.getLiftRideFromMessage(message);
          int skierId = LiftRideConsumer.getSkierIdFromMessage(message);

          // Update to HashMap
          skierLiftRides.compute(skierId, (key, liftRidesList) -> {
            if (liftRidesList == null) {
              liftRidesList = Collections.synchronizedList(new ArrayList<>());
            }
            liftRidesList.add(liftRide);
            return liftRidesList;
          });
        } catch (Exception e) {
          System.err.println("Failed to process message: " + message);
          e.printStackTrace();
        }
      };
      channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (channel != null) {
        channelPool.offer(channel); // Return the channel to the pool
      }
    }
  }
}