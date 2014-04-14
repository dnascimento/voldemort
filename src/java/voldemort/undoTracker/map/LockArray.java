/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */
package voldemort.undoTracker.map;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class LockArray<K> {

    private final Logger log = LogManager.getLogger(LockArray.class.getName());
    public final ReentrantLock[] partitionMutex;
    public final int N_PARTITIONS;

    public LockArray(int size) {
        N_PARTITIONS = size;
        partitionMutex = new ReentrantLock[size];
        for(int i = 0; i < size; i++) {
            partitionMutex[i] = new ReentrantLock();
        }
    }

    public void lock(K key) {
        int p = Math.abs(key.hashCode()) % N_PARTITIONS;
        partitionMutex[p].lock();
        log.debug("lock: " + p);
    }

    public void release(K key) {
        int p = Math.abs(key.hashCode()) % N_PARTITIONS;
        log.debug("release: " + p);
        partitionMutex[p].unlock();
    }

    /**
     * Try to adquire every mutex in the array (may have a big delay due to
     * starvation)
     */
    public void lockAllMutex() {
        for(int i = 0; i < N_PARTITIONS; i++) {
            partitionMutex[i].lock();
        }
    }

    public void releaseAllMutex() {
        for(int i = 0; i < N_PARTITIONS; i++) {
            partitionMutex[i].unlock();
        }
    }

}
