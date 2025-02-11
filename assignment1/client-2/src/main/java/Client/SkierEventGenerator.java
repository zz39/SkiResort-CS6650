package Client;

import io.swagger.client.model.LiftRide;
import java.util.Random;
import java.util.concurrent.*;


public class SkierEventGenerator implements Runnable {

  private BlockingDeque<LiftRide> queue;
  private int numEvent;
  static private final int LIFT_ID_RANGE = 40;
  static private final int TIME_RANGE = 360;

  public SkierEventGenerator(int numEvent, BlockingDeque<LiftRide> queue) {
    this.numEvent = numEvent;
    this.queue = queue;
  }

  public BlockingDeque<LiftRide> getQueue() {
    return queue;
  }

  public int getNumEvent() {
    return numEvent;
  }

  private LiftRide createRandomLiftRide() {
    Random random = new Random();
    LiftRide liftRide = new LiftRide();
    liftRide.setLiftID(random.nextInt(LIFT_ID_RANGE) + 1);
    liftRide.setTime(random.nextInt(TIME_RANGE) + 1);
    return liftRide;
  }

  @Override
  public void run() {
    System.out.println("SkierEventGenerator starting...");
    for (int i = 0; i < numEvent; i++) {
      LiftRide newLiftRide = createRandomLiftRide();
      try {
        queue.put(newLiftRide);
        if (i % 10 == 0) {
          System.out.println("Generated " + (i + 1) + " events");
        }
      } catch (InterruptedException e) {
        System.err.println("Interrupted after generating " + i + " events");
        Thread.currentThread().interrupt();
        return;
      }
    }
    System.out.println("SkierEventGenerator finished. Generated " + numEvent + " events");
  }
}