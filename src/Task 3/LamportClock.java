import java.util.*;
import java.util.concurrent.*;

public class LamportClock {
   private int c = 0;

   public void localStep() {
      c = c + 1;
   }

   public void sendEvent() {
      c = c + 1;
   }

   public void receiveEvent(int sentValue) {
      c = Math.max(c, sentValue) + 1;
   }

   public int getValue() {
      return c;
   }

   private static final ConcurrentHashMap<Integer, BlockingQueue<Integer>> channels = new ConcurrentHashMap<>();

   static class EventConfig {
      int processId;
      int eventId;
      int relationshipInput;

      EventConfig(int processId, int eventId, int relationshipInput) {
         this.processId = processId;
         this.eventId = eventId;
         this.relationshipInput = relationshipInput;
      }
   }

   static class ProcessThread implements Runnable {
      int pId;
      List<EventConfig> events;
      LamportClock clock = new LamportClock();
      int[] timestamps;

      ProcessThread(int pId, List<EventConfig> events) {
         this.pId = pId;
         this.events = events;
         this.timestamps = new int[events.size() + 1];
      }

      @Override
      public void run() {
         for (EventConfig event : events) {
            int eNum = event.eventId;
            int input = event.relationshipInput;

            if (input == 0) {
               clock.localStep();
               timestamps[eNum] = clock.getValue();
            } else {
               try {
                  BlockingQueue<Integer> queue = channels.get(input);
                  int sentClock = queue.take();
                  clock.receiveEvent(sentClock);
                  timestamps[eNum] = clock.getValue();
               } catch (InterruptedException ex) {
                  Thread.currentThread().interrupt();
               }
            }

            int myEventKey = pId * 10 + eNum;
            if (channels.containsKey(myEventKey)) {
               channels.get(myEventKey).offer(timestamps[eNum]);
            }
         }
      }
   }

   public static void main(String[] args) throws InterruptedException {
      Scanner sc = new Scanner(System.in);

      System.out.print("Enter the number of processes: ");
      int p = sc.nextInt();
      int[] ev = new int[p + 1];

      for (int i = 1; i <= p; i++) {
         System.out.print("Enter the number of events for process " + i + ": ");
         ev[i] = sc.nextInt();
      }

      System.out.println("Enter the relationship (0 for intra-process, or event number of other process:");

      Map<Integer, List<EventConfig>> processEvents = new HashMap<>();

      for (int i = 1; i <= p; i++) {
         System.out.println("For process" + i + "...");
         List<EventConfig> events = new ArrayList<>();
         for (int j = 1; j <= ev[i]; j++) {
            System.out.print("For event" + j + ": ");
            int input = sc.nextInt();
            events.add(new EventConfig(i, j, input));

            if (input != 0) {
               channels.putIfAbsent(input, new LinkedBlockingQueue<>());
            }
         }
         processEvents.put(i, events);
      }

      List<ProcessThread> workers = new ArrayList<>();
      List<Thread> threads = new ArrayList<>();

      for (int i = 1; i <= p; i++) {
         ProcessThread worker = new ProcessThread(i, processEvents.get(i));
         workers.add(worker);
         Thread t = new Thread(worker);
         threads.add(t);
         t.start();
      }

      for (Thread t : threads) {
         t.join();
      }

      System.out.println("\n--- Final Logical Timestamps ---");
      for (ProcessThread worker : workers) {
         for (int j = 1; j <= ev[worker.pId]; j++) {
            System.out.println("e" + worker.pId + j + ": " + worker.timestamps[j]);
         }
      }

      sc.close();
   }
}