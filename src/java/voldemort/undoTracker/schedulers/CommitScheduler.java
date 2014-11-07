/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import java.io.Serializable;

import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

/**
 * @author darionascimento
 * 
 */
public class CommitScheduler extends AccessSchedule implements Serializable {

    private static final long serialVersionUID = 1L;

    private OpMultimap keyAccessLists;

    /**
     * Each request handler has a UndoStub instance
     */
    public CommitScheduler(OpMultimap keyAccessLists) {
        this.keyAccessLists = keyAccessLists;
    }

    /**
     * 
     * @param key
     * @param srd
     * @param current
     * @param branch
     * @return the key version to access
     */
    @Override
    public StsBranchPair getStart(ByteArray key, SRD srd, BranchPath current) {
        return keyAccessLists.trackReadAccess(key, srd, current);
    }

    @Override
    public StsBranchPair putStart(ByteArray key, SRD srd, BranchPath path) {
        return keyAccessLists.trackWriteAccess(key, Op.OpType.Put, srd, path);
    }

    @Override
    public StsBranchPair deleteStart(ByteArray key, SRD srd, BranchPath path) {
        return keyAccessLists.trackWriteAccess(key, Op.OpType.Delete, srd, path);
    }

    @Override
    public StsBranchPair getVersionStart(ByteArray key, SRD srd, BranchPath current) {
        return keyAccessLists.getVersionToPut(key, srd, current);
    }

    @Override
    public void getEnd(ByteArray key, SRD srd, BranchPath path) {
        keyAccessLists.endReadAccess(key);
    }

    @Override
    public void putEnd(ByteArray key, SRD srd, BranchPath path) {
        keyAccessLists.endWriteAccess(key);
    }

    @Override
    public void deleteEnd(ByteArray key, SRD srd, BranchPath path) {
        keyAccessLists.endWriteAccess(key);
    }

    @Override
    void getVersionEnd(ByteArray key, SRD srd, BranchPath path) {
        keyAccessLists.endReadAccess(key);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        CommitScheduler other = (CommitScheduler) obj;
        if(keyAccessLists == null) {
            if(other.keyAccessLists != null)
                return false;
        } else if(!keyAccessLists.equals(other.keyAccessLists))
            return false;
        return true;
    }

}
