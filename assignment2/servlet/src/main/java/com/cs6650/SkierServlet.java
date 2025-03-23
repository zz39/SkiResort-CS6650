package com.cs6650;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;
import java.io.IOException;
import com.rabbitmq.client.*;

@WebServlet("/skiers/*")
public class SkierServlet extends HttpServlet {

  private final static String QUEUE_NAME = "SkierQueue";
  private static final String RABBITMQ_HOST = "13.216.71.149"; // Elastic IP of RabbitMQ EC2
  private static final int CHANNEL_POOL_SIZE = 120;
  private static ConnectionFactory factory = new ConnectionFactory();
  private static Connection connection;
  private static final BlockingQueue<Channel> channelPool = new LinkedBlockingQueue<>(CHANNEL_POOL_SIZE);

  static { initializeChannelPool(); }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();
    System.out.println(urlPath);

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts, true)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write("get ok!");
    }
  }

  private boolean isUrlValid(String[] urlPath, boolean isGet) {
    if (isGet) return true;

    if (urlPath.length < 6) return false;

    try {
      Integer.parseInt(urlPath[1]);
      Integer.parseInt(urlPath[3]);
      Integer.parseInt(urlPath[5]);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts, false)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      String body = req.getReader().lines().collect(Collectors.joining());
      if (body.isEmpty()) {
        res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      } else {
        String skierID = urlParts[urlParts.length - 1];
        String message = packageMessage(body, skierID);

        // Send data to RabbitMQ message queue
        boolean success = sendToMessageQueue(message);
        if (success) {
          res.setStatus(HttpServletResponse.SC_CREATED);
          res.getWriter().write("POST ok!");
        } else {
          res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          res.getWriter().write("Failed to send message to queue");
        }
      }
    }
  }

  private static void initializeChannelPool() {
    factory.setHost(RABBITMQ_HOST);
    try {
      connection = factory.newConnection();
      Channel setupChannel = connection.createChannel();
      setupChannel.queueDeclare(QUEUE_NAME, true, false, false, null); // Durable queue
      setupChannel.close();

      for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
        Channel channel = connection.createChannel();
        channelPool.add(channel);
      }
    } catch (IOException | TimeoutException e) {
      throw new RuntimeException("Failed to initialize RabbitMQ channel pool", e);
    }
  }

  private String packageMessage(String body, String skierID) {
    return "{\"body\":" + body + ", \"skierID\":\"" + skierID + "\"}";
  }

  private boolean sendToMessageQueue(String message) {
    Channel channel = null;
    try {
      channel = channelPool.poll();
      if (channel == null) {
        System.err.println("No available channel in pool!");
        return false;
      }

      channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
      System.out.println(" [x] Sent '" + message + "'");
      return true;

    } catch (IOException e) {
      System.err.println("Failed to send message to queue: " + e.getMessage());
      return false;
    } finally {
      if (channel != null) {
        channelPool.offer(channel); // Return channel to pool
      }
    }
  }
}