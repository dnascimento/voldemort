package voldemort;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import voldemort.undoTracker.map.RWLock;

public class RWLockTest {

    private class Exec extends Thread {

        RWLock lock;
        private boolean write;

        public Exec(RWLock lock, boolean write) {
            super();
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
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        public void write() throws InterruptedException {
            System.out.println(getId() + ": Try write");
            lock.lockWrite();
            System.out.println(getId() + " : Writting...");
            sleep(1000);
            System.out.println(getId() + " : Try release write");
            lock.releaseWrite();
            System.out.println(getId() + " : Released write");
        }

        public void read() throws InterruptedException {
            System.out.println(getId() + " : Try read");
            lock.lockRead();
            System.out.println(getId() + " : Reading...");
            sleep(1000);
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

    @Test
    public void testRWWaiting() {

    }
}
