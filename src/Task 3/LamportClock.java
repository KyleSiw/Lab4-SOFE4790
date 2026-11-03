public class LamportClock {
   int c;

   // Each process keeps an integer initially 0
   public LamportClock() {
      c = 0;
   }

   public int getValue() {
      return c;
   }

   // Internal even or local step  
   public void localStep() {
      c = c + 1;
   }

   public void sendEvent() {
      c = c + 1;
   }

   public void receiveEvent(int src, int sentValue) {
      c = max(c, sentValue) + 1;
   }
    
   public int max(int a, int b) {
      if(a>b) {
         return a;
      } else {
         return b;
      }
   }
}
