/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.RUD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

/**
 * @author darionascimento
 * 
 */
public class CommitScheduler extends AccessSchedule {

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
     * @param rud
     * @param current
     * @param branch
     * @return the key version to access
     */
    @Override
    public StsBranchPair getStart(ByteArray key, RUD rud, BranchPath current) {
        return keyAccessLists.trackReadAccess(key, rud, current);
    }

    @Override
    public StsBranchPair putStart(ByteArray key, RUD rud, BranchPath path) {
        return keyAccessLists.trackWriteAccess(key, Op.OpType.Put, rud, path);
    }

    @Override
    public StsBranchPair deleteStart(ByteArray key, RUD rud, BranchPath path) {
        return keyAccessLists.trackWriteAccess(key, Op.OpType.Delete, rud, path);
    }

    @Override
    public StsBranchPair getVersionStart(ByteArray key, RUD rud, BranchPath current) {
        return keyAccessLists.getVersionToPut(key, rud, current);
    }

    @Override
    public void getEnd(ByteArray key, RUD rud, BranchPath path) {
        keyAccessLists.endReadAccess(key);
    }

    @Override
    public void putEnd(ByteArray key, RUD rud, BranchPath path) {
        keyAccessLists.endWriteAccess(key);
    }

    @Override
    public void deleteEnd(ByteArray key, RUD rud, BranchPath path) {
        keyAccessLists.endWriteAccess(key);
    }

}
