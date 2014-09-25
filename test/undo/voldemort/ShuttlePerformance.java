package voldemort;

import java.util.Random;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchController;
import voldemort.utils.ByteArray;

public class ShuttlePerformance extends Thread {

    private static final int THREADS = 10;
    static DBProxy proxy = new DBProxy();
    static final int KEY_RANGE = 10;
    static final int TOTAL_REQUESTS = 2000000;
    static final short BRANCH = BranchController.INIT_BRANCH;

    int id;

    public ShuttlePerformance(int i) {
        this.id = i;
    }

    public static void main(String[] args) {
        int i = 0;
        Thread[] threads = new Thread[THREADS];
        for(i = 0; i < threads.length; i++) {
            threads[i] = new ShuttlePerformance(i);
        }

        long start = System.nanoTime();

        for(i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for(i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
        long duration = ((System.nanoTime() - start) / 1000000);
        System.out.println("TOTAL: Duration (miliseconds): " + duration);
        System.out.println("TOTAL: Rate (req/seg)"
                           + ((double) THREADS * (TOTAL_REQUESTS) / duration * 1000));

    }

    @Override
    public void run() {
        Random random = new Random();
        for(int i = 0; i < TOTAL_REQUESTS; i++) {
            // generate key
            String keyText = new Integer((int) (random.nextDouble() * KEY_RANGE)).toString();
            ByteArray key = new ByteArray(keyText.getBytes());

            // generate srd
            SRD srd = new SRD(System.currentTimeMillis(), BRANCH, false);

            // switch to operation
            if(random.nextBoolean()) {
                proxy.getStart(key, srd);
                proxy.getEnd(key, srd);
            } else {
                proxy.putStart(key, srd);
                proxy.putEnd(key, srd);
            }
        }
    }

}
