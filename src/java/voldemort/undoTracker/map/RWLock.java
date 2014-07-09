/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.map;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.log4j.Logger;

/**
 * Read/Write lock ordered by arrival time.
 * 
 * @author darionascimento
 * 
 */
public class RWLock {

    private enum LockOp implements Serializable {
        READ,
        WRITE;
    }

    // TODO lock access politic may cause issues

    private Object readsWaiter = new Object();
    private Object writesWaiter = new Object();

    private Object readsLock = new Object();
    private Object writesLock = new Object();

    private int readsWaiting = 0;
    private int readsExec = 0;

    private int writesWaiting = 0;
    private int writesExec = 0;

    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedList<LockOp> opsQueue = new LinkedList<LockOp>();
    private transient static final Logger log = Logger.getLogger(RWLock.class.getName());

    public RWLock() {
        log.debug("New lock");
    }

    public void notifyWrites() {
        lock.lock();

        writesWaiting = 0;
        synchronized(writesWaiter) {
            writesWaiter.notifyAll();
        }

        lock.unlock();
    }

    public void notifyReads() {
        lock.lock();

        readsWaiting = 0;
        synchronized(readsWaiter) {
            readsWaiter.notifyAll();
        }

        lock.unlock();
    }

    public void readWait() {
        lock.lock();

        readsWaiting++;
        try {
            synchronized(readsWaiter) {
                lock.unlock();
                readsWaiter.wait();
            }
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
        lock.lock();
        readsWaiting--;
        lock.unlock();
    }

    public void writeWait() {
        lock.lock();
        writesWaiting++;
        try {
            synchronized(writesWaiter) {
                lock.unlock();
                writesWaiter.wait();
            }
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
        lock.lock();
        writesWaiting--;
        lock.unlock();
    }

    // --------------------------------------------------

    public void lockWrite() {
        lock.lock();
        while(readsExec > 0 || writesExec > 0) {
            try {
                opsQueue.addLast(LockOp.WRITE);
                synchronized(writesLock) {
                    lock.unlock();
                    writesLock.wait();
                    lock.lock();
                }
                opsQueue.remove(LockOp.WRITE);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
        writesExec++;
        assert (writesExec == 1);
        lock.unlock();
    }

    public void lockRead() {
        lock.lock();

        while(writesExec > 0) {
            try {
                opsQueue.addLast(LockOp.READ);
                synchronized(readsLock) {
                    lock.unlock();
                    readsLock.wait();
                    lock.lock();
                }
                opsQueue.remove(LockOp.READ);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
        readsExec++;
        lock.unlock();
    }

    public void releaseWrite() {
        lock.lock();
        writesExec--;
        assert (writesExec == 0);
        unlockNext();
        lock.unlock();
    }

    public void releaseRead() {
        lock.lock();
        readsExec--;
        if(readsExec == 0) {
            unlockNext();
        }
        lock.unlock();
    }

    /**
     * Lock is free, unlock someone
     */
    private void unlockNext() {
        Iterator<LockOp> it = opsQueue.iterator();
        if(!it.hasNext())
            return;

        // printQueue();
        LockOp op = it.next();
        // Wake one write or a batch of reads until next write
        if(op.equals(LockOp.WRITE)) {
            // unlock next write
            synchronized(writesLock) {
                writesLock.notify();
            }
        } else {
            synchronized(readsLock) {
                readsLock.notify();
                while(it.hasNext()) {
                    op = it.next();
                    if(op.equals(LockOp.WRITE)) {
                        break;
                    }
                    readsLock.notify();
                }
            }
        }
    }

    /**
     * Count if there are some operation running
     * 
     * @return
     */
    public boolean hasPendent() {
        lock.lock();
        int count = writesExec + readsExec + opsQueue.size() + writesWaiting + readsWaiting;
        lock.unlock();
        return count != 0;
    }

    @SuppressWarnings("unused")
    private void printQueue() {
        for(LockOp o: opsQueue) {
            System.out.print(o);
            System.out.print(" : ");
        }
        System.out.print("\n");
    }
}
