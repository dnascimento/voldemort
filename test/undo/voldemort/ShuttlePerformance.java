package voldemort;

import java.util.Random;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchController;
import voldemort.utils.ByteArray;

public class ShuttlePerformance {

    static DBProxy proxy = new DBProxy();
    static int KEY_RANGE = 2000;
    static int TOTAL_REQUESTS = 20000000;
    static short BRANCH = BranchController.INIT_BRANCH;

    public static void main(String[] args) {
        Random random = new Random();
        int baseRid = 10;
        long start = System.nanoTime();
        for(int i = 0; i < TOTAL_REQUESTS; i++) {
            // generate key
            String keyText = new Integer((int) (random.nextDouble() * KEY_RANGE)).toString();
            ByteArray key = new ByteArray(keyText.getBytes());

            // generate srd
            SRD srd = new SRD(baseRid + i, BRANCH, false);

            // switch to operation
            if(random.nextBoolean()) {
                proxy.getStart(key, srd);
                proxy.getEnd(key, srd);
            } else {
                proxy.putStart(key, srd);
                proxy.putEnd(key, srd);
            }
        }
        long duration = (System.nanoTime() - start) / 1000;
        double rate = TOTAL_REQUESTS / (duration / 1000);
        System.out.println("Duration: " + duration + " rate (req/ms): " + rate);
    }

}
