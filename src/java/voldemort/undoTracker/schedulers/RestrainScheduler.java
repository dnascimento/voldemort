/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import java.io.Serializable;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

/**
 * 
 * @author darionascimento
 * 
 */
public class RestrainScheduler extends AccessSchedule implements Serializable {

    private static final long serialVersionUID = 1L;

    private final transient Logger log = Logger.getLogger(RestrainScheduler.class.getName());

    private ReentrantLock flag;

    public RestrainScheduler(OpMultimap keyOperationsMultimap, ReentrantLock restrainLocker) {
        super(keyOperationsMultimap);
        this.flag = restrainLocker;
    }

    @Override
    public void getEnd(ByteArray key, SRD srd, BranchPath path) {}

    @Override
    public void putEnd(ByteArray key, SRD srd, BranchPath path) {}

    @Override
    public void deleteEnd(ByteArray key, SRD srd, BranchPath path) {}

    @Override
    void getVersionEnd(ByteArray key, SRD srd, BranchPath path) {}

    @Override
    public StsBranchPair getStart(ByteArray key, SRD srd, BranchPath path) {
        // OpMultimapEntry l = keyOperationsMultimap.get(key);
        // if(l.isReplayingInBranch(path.current.branch)) {
        synchronized(flag) {
            try {
                flag.wait();
            } catch(InterruptedException e) {
                log.error("Restrain Wait in flag", e);
            }
            // }
        }
        return null;
    }

    @Override
    public StsBranchPair putStart(ByteArray key, SRD srd, BranchPath path) {
        // OpMultimapEntry l = keyOperationsMultimap.get(key);
        // if(l.isReplayingInBranch(path.current.branch)) {
        synchronized(flag) {
            try {
                flag.wait();
            } catch(InterruptedException e) {
                log.error("Restrain Wait in flag", e);
            }
        }
        // }
        return null;
    }

    @Override
    public StsBranchPair deleteStart(ByteArray key, SRD srd, BranchPath path) {
        // OpMultimapEntry l = keyOperationsMultimap.get(key);
        // if(l.isReplayingInBranch(path.current.branch)) {
        synchronized(flag) {
            try {
                flag.wait();
            } catch(InterruptedException e) {
                log.error("Restrain Wait in flag", e);
            }
        }
        // }
        return null;
    }

    @Override
    public StsBranchPair getVersionStart(ByteArray key, SRD srd, BranchPath path) {
        // OpMultimapEntry l = keyOperationsMultimap.get(key);
        // if(l.isReplayingInBranch(path.current.branch)) {
        synchronized(flag) {
            try {
                flag.wait();
            } catch(InterruptedException e) {
                log.error("Restrain Wait in flag", e);
            }
        }
        // }
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        RestrainScheduler other = (RestrainScheduler) obj;
        if(keyOperationsMultimap == null) {
            if(other.keyOperationsMultimap != null)
                return false;
        } else if(!keyOperationsMultimap.equals(other.keyOperationsMultimap))
            return false;
        return true;
    }

}
