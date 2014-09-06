package voldemort.old;

import java.util.Random;

import junit.framework.Assert;

import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Test;

import voldemort.undoTracker.map.LockArray;

/**
 * Test an array of locks. Creates N_Theads in a Y partionated array of locks.
 * 
 * @author darionascimento
 * 
 */
public class LockArrayTest {

    private LockArray<Integer> lock = new LockArray<Integer>(2);
    private int N_THREADS = 10;
    private int N_KEYs = 4;

    // true when get in, false when get out
    static boolean[] trackInOut;

    public class AccessLock extends Thread {

        int key; // -1 for all

        public AccessLock(int key) {
            super();
            this.key = key;
        }

        @Override
        public void run() {
            try {

                if(key == -1) {
                    lock.lockAllMutex();
                    registerAccess(key, true);
                    System.out.println("In: all");
                    Thread.sleep(1000);
                    System.out.println("Out: all");
                    registerAccess(key, false);
                    lock.releaseAllMutex();
                } else {
                    lock.lock(key);
                    registerAccess(key, true);
                    System.out.println("In");
                    Thread.sleep(1000);
                    System.out.println("Out");
                    registerAccess(key, false);
                    lock.release(key);
                }
            } catch(InterruptedException e) {
                System.err.println(e);
                Assert.assertTrue(false);
            }
        }
    }

    /**
     * Create a set of threads which will access the lock in parallel.
     * Each thread writes true and then false during the critical section
     * If there are two trues or two false in line, then two threads shared the
     * critical section
     * 
     * @throws InterruptedException
     */
    @Test
    public void test() throws InterruptedException {
        DOMConfigurator.configure("log4j.xml");
        Random r = new Random();

        // create N_THREADS to access N_KEYs random keys
        Thread[] threads = new Thread[N_THREADS];
        int max = 0;

        for(int i = 0; i < N_THREADS; i++) {
            int index = r.nextInt(N_KEYs);
            if(index > max) {
                max = index;
            }
            threads[i] = new AccessLock(r.nextInt(N_KEYs));
        }

        trackInOut = new boolean[max + 1];

        for(int i = 0; i < N_THREADS; i++) {
            threads[i].start();
        }
        // -1 locks all and releases all
        new AccessLock(-1).start();

        for(int i = 0; i < N_THREADS; i++) {
            threads[i].join();
        }
    }

    /**
     * Register true if in and false if out. if both are in or both are getting
     * out, then there is a problem.
     * 
     * @param index
     * @param in
     */
    public synchronized void registerAccess(int index, boolean in) {
        if(index == -1) {
            for(int i = 0; i < trackInOut.length; i++) {
                Assert.assertFalse(trackInOut[i]);
            }
        } else {
            Assert.assertFalse(trackInOut[index] == in);
            trackInOut[index] = in;
        }
    }
}
