import java.util.*;
import java.util.concurrent.*;

public class VectorClock {
   int[] v;
   int myId;
   int N;

   public VectorClock(int numProc, int id) {
      myId = id;
      N = numProc;
      v = new int[numProc];
      for (int i = 0; i < N; i++) {
         v[i] = 0;
      }
   }

   public void localStep() {
      v[myId]++;
   }

   public void sendEvent() {
      v[myId]++;
   }

   public void receiveEvent(int[] sentValue) {
      for (int i = 0; i < N; i++) {
         v[i] = max(v[i], sentValue[i]);
      }
      v[myId]++;
   }

   public int getValue(int i) {
      return v[i];
   }

   public int[] getVector() {
      return v.clone();
   }

   public int max(int a, int b) {
      if (a > b) {
         return a;
      } else {
         return b;
      }
   }

   public String toString() {
      StringBuilder s = new StringBuilder("[");
      for (int j = 0; j < v.length; j++) {
         s.append(v[j]).append(j == v.length - 1 ? "" : ", ");
      }
      s.append("]");
      return s.toString();
   }

   private static final ConcurrentHashMap<Integer, BlockingQueue<int[]>> channels = new ConcurrentHashMap<>();

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
      int totalProcesses;
      List<EventConfig> events;
      VectorClock clock;
      String[] timestamps;

      ProcessThread(int pId, int totalProcesses, List<EventConfig> events) {
         this.pId = pId;
         this.totalProcesses = totalProcesses;
         this.events = events;
         this.clock = new VectorClock(totalProcesses, pId - 1);
         this.timestamps = new String[events.size() + 1];
      }

      @Override
      public void run() {
         for (EventConfig event : events) {
            int eNum = event.eventId;
            int input = event.relationshipInput;

            if (input == 0) {
               clock.localStep();
               timestamps[eNum] = clock.toString();
            } else {
               try {
                  BlockingQueue<int[]> queue = channels.get(input);
                  int[] sentVector = queue.take();
                  clock.receiveEvent(sentVector);
                  timestamps[eNum] = clock.toString();
               } catch (InterruptedException ex) {
                  Thread.currentThread().interrupt();
               }
            }

            int myEventKey = pId * 10 + eNum;
            if (channels.containsKey(myEventKey)) {
               channels.get(myEventKey).offer(clock.getVector());
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
         ProcessThread worker = new ProcessThread(i, p, processEvents.get(i));
         workers.add(worker);
         Thread t = new Thread(worker);
         threads.add(t);
         t.start();
      }

      for (Thread t : threads) {
         t.join();
      }

      System.out.println("\n--- Final Vector Timestamps ---");
      for (ProcessThread worker : workers) {
         for (int j = 1; j <= ev[worker.pId]; j++) {
            System.out.println("e" + worker.pId + j + ": " + worker.timestamps[j]);
         }
      }

      sc.close();
   }
}