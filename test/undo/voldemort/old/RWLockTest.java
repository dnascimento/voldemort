package voldemort.old;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.undoTracker.map.RWLock;

/**
 * Check if the read-write lock allows multiple reads but a single write
 * 
 * @author darionascimento
 * 
 */
public class RWLockTest {

    /*
     * Negative: write
     * Zero: none
     * Positive: num of readers
     */
    static int state = 0;

    public class Exec extends Thread {

        RWLock lock;
        private boolean write;

        public Exec(RWLock lock, boolean write) {
            this.lock = lock;
            this.write = write;
        }

        @Override
        public void run() {
            try {
                if(write) {
                    write();
                } else {
                    read();
                }
            } catch(InterruptedException e) {
                System.err.println(e);
                Assert.assertFalse(true);
            }
        }

        public void write() throws InterruptedException {
            System.out.println(getId() + ": Try write");
            lock.lockWrite();
            Assert.assertEquals(0, state);
            state--;
            System.out.println(getId() + " : Writting...");
            sleep(1000);
            Assert.assertEquals(-1, state);
            state++;
            System.out.println(getId() + " : Try release write");
            lock.releaseWrite();
            System.out.println(getId() + " : Released write");
        }

        public void read() throws InterruptedException {
            System.out.println(getId() + " : Try read");
            lock.lockRead();
            Assert.assertTrue(state >= 0);
            state++;
            System.out.println(getId() + " : Reading...");
            sleep(1000);
            Assert.assertTrue(state > 0);
            state--;
            System.out.println(getId() + " : Try release read");
            lock.releaseRead();
            System.out.println(getId() + " : Released read");
        }
    }

    @Test
    public void testRWLock() throws InterruptedException {
        RWLock lock = new RWLock();
        List<Thread> ts = new ArrayList<Thread>();
        ts.add(new Exec(lock, true));
        ts.add(new Exec(lock, false));
        ts.add(new Exec(lock, false));
        ts.add(new Exec(lock, true));
        ts.add(new Exec(lock, true));
        ts.add(new Exec(lock, false));
        ts.add(new Exec(lock, false));
        ts.add(new Exec(lock, true));

        for(Thread t: ts) {
            t.start();
        }
        for(Thread t: ts) {
            t.join();
        }
    }

}
