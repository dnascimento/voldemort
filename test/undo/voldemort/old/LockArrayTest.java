package voldemort.old;

import java.util.Random;

import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Test;

import voldemort.undoTracker.map.LockArray;

public class LockArrayTest {

    private LockArray<Integer> lock = new LockArray<Integer>(2);
    private int N_THREADS = 10;
    private int N_KEYs = 4;

    public class AccessLock extends Thread {

        int key; // -1 for all

        public AccessLock(int key) {
            super();
            this.key = key;
        }

        @Override
        public void run() {
            if(key == -1) {
                lock.lockAllMutex();
                System.out.println("Open: All");
                lock.releaseAllMutex();
                System.out.println("Close: all");
            } else {
                lock.lock(key);
                System.out.println("Open: " + key);
                lock.release(key);
                System.out.println("close: " + key);
            }
        }
    }

    @Test
    public void test() throws InterruptedException {
        DOMConfigurator.configure("log4j.xml");
        Random r = new Random();
        Thread[] threads = new Thread[N_THREADS];
        for(int i = 0; i < N_THREADS; i++) {
            threads[i] = new AccessLock(r.nextInt(N_KEYs));
        }

        new AccessLock(-1).start();
        for(int i = 0; i < N_THREADS; i++) {
            threads[i].start();
        }
        for(int i = 0; i < N_THREADS; i++) {
            threads[i].join();
        }
    }
}
