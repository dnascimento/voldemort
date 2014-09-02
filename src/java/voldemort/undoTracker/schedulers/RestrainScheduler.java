/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import org.apache.log4j.Logger;

import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.OpMultimapEntry;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

/**
 * 
 * @author darionascimento
 * 
 */
public class RestrainScheduler extends AccessSchedule {

    private final Logger log = Logger.getLogger(RestrainScheduler.class.getName());

    OpMultimap archive;
    private Object flag;

    public RestrainScheduler(OpMultimap archive, Object restrainLocker) {
        super();
        this.archive = archive;
        this.flag = restrainLocker;
    }

    @Override
    public void getEnd(ByteArray key, SRD srd, BranchPath path) {}

    @Override
    public void putEnd(ByteArray key, SRD srd, BranchPath path) {}

    @Override
    public void deleteEnd(ByteArray key, SRD srd, BranchPath path) {}

    @Override
    public StsBranchPair getStart(ByteArray key, SRD srd, BranchPath path) {
        OpMultimapEntry l = archive.get(key);
        if(l.isReplayingInBranch(path.current.branch)) {
            synchronized(flag) {
                try {
                    flag.wait();
                } catch(InterruptedException e) {
                    log.error("Restrain Wait in flag", e);
                }
            }
        }
        return null;
    }

    @Override
    public StsBranchPair putStart(ByteArray key, SRD srd, BranchPath path) {
        OpMultimapEntry l = archive.get(key);
        if(l.isReplayingInBranch(path.current.branch)) {
            synchronized(flag) {
                try {
                    flag.wait();
                } catch(InterruptedException e) {
                    log.error("Restrain Wait in flag", e);
                }
            }
        }
        return null;
    }

    @Override
    public StsBranchPair deleteStart(ByteArray key, SRD srd, BranchPath path) {
        OpMultimapEntry l = archive.get(key);
        if(l.isReplayingInBranch(path.current.branch)) {
            synchronized(flag) {
                try {
                    flag.wait();
                } catch(InterruptedException e) {
                    log.error("Restrain Wait in flag", e);
                }
            }
        }
        return null;
    }

    @Override
    public StsBranchPair getVersionStart(ByteArray key, SRD srd, BranchPath path) {
        OpMultimapEntry l = archive.get(key);
        if(l.isReplayingInBranch(path.current.branch)) {
            synchronized(flag) {
                try {
                    flag.wait();
                } catch(InterruptedException e) {
                    log.error("Restrain Wait in flag", e);
                }
            }
        }
        return null;
    }
}
