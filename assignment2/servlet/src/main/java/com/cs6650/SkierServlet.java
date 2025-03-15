package com.cs6650;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;
import java.io.IOException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

@WebServlet("/skiers/*")
public class SkierServlet extends HttpServlet {

  private final static String QUEUE_NAME = "SkierQueue";
  private static ConnectionFactory factory = new ConnectionFactory();
  private static final int CHANNEL_POOL_SIZE = 120;
  private static final BlockingQueue<Channel> channelPool = new LinkedBlockingQueue<>(CHANNEL_POOL_SIZE);

  static { initializeChannelPool();}

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();
    System.out.println(urlPath);

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts, false)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write("get ok!");
    }
  }

  private boolean isUrlValid(String[] urlPath, Boolean get) {
    if (get) {
      return true;
    }

    if (urlPath.length < 6) {  // Ensure sufficient elements exist
      return false;
    }

    try {
      Integer.parseInt(urlPath[1]);
      Integer.parseInt(urlPath[3]);
      Integer.parseInt(urlPath[5]);
      return true;
    } catch(NumberFormatException e) {
      return false;
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts, false)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      String body = String.valueOf(req.getReader().lines().collect(Collectors.joining()));
      if (body.isEmpty()) {
        res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      } else {
        String skierID = urlParts[urlParts.length - 1];
        String message = packageMessage(body, skierID);

        // Send data to RabbitMQ message queue
        sendToMessageQueue(message);
        res.setStatus(HttpServletResponse.SC_CREATED);
        res.getWriter().write("POST ok!");
      }
    }
  }

  private static void initializeChannelPool() {
    factory.setHost("34.225.55.133"); // Elastic EC2 Public IP
    try {
      Connection connection = factory.newConnection();
      for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channelPool.add(channel);
      }
    } catch (IOException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  // Package liftRide with skierID in JSON
  private String packageMessage(String body, String skierID) {
    return "{\"body\":" + body + ", \"skierID\":\"" + skierID + "\"}";
  }

  private void sendToMessageQueue(String message) {
    Channel channel = channelPool.poll();
    if (channel == null) {
      System.err.println("No available RabbitMQ channel in pool!");
      return;
    }
    try {
      channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
      System.out.println(" [x] Sent '" + message + "'");
    } catch (IOException e) {
      throw new RuntimeException("Failed to send message to RabbitMQ", e);
    } finally {
      channelPool.offer(channel); // Return channel to the pool safely
    }
  }

  @Override
  public void destroy() {
    for (Channel channel : channelPool) {
      try {
        channel.close();
      } catch (IOException | TimeoutException e) {
        System.err.println("Failed to close channel: " + e.getMessage());
      }
    }
  }
}