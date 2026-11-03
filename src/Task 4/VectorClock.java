public class VectorClock {
   int[] v;
   int myId;
   int N;

   //  myId is the index of this process in the vector: 0, 1, ..., numProc-1
   //  Every entry starts at 0, like the Lamport clock in LamportClock.java
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

   //  Returns a copy of the vector: send the copy with a message, not v itself,
   //  or the receiver will see the sender's later changes
   public int[] getVector() {
      return v.clone();
   }

   public int max(int a, int b) {
      if(a>b) {
         return a;
      } else {
         return b;
      }
   }

   public String toString(){
      StringBuffer s = new StringBuffer();
      for (int j = 0; j < v.length; j++) {
         s.append(String.valueOf(v[j]) + " ");
      }
      return new String(s.toString());
   }
}
