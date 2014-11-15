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
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

/**
 * 
 * @author darionascimento
 * 
 */
public class RedoScheduler extends AccessSchedule implements Serializable {

    private static final long serialVersionUID = 1L;

    public RedoScheduler(OpMultimap keyOperationsMultimap) {
        super(keyOperationsMultimap);
    }

    /*
     * reads are perform in new branch or the base commit branch
     */
    @Override
    public StsBranchPair getStart(ByteArray key, SRD srd, BranchPath path) {
        return keyOperationsMultimap.get(key).redoRead(OpType.Get, srd, path);
    }

    /*
     * writes are perform in new branch
     */
    @Override
    public StsBranchPair putStart(ByteArray key, SRD srd, BranchPath path) {
        return keyOperationsMultimap.get(key).redoWrite(OpType.Put, srd, path);
    }

    /*
     * writes are perform in new branch
     */
    @Override
    public StsBranchPair deleteStart(ByteArray key, SRD srd, BranchPath path) {
        return keyOperationsMultimap.get(key).redoWrite(OpType.Delete, srd, path);
    }

    @Override
    public void getEnd(ByteArray key, SRD srd, BranchPath path) {
        keyOperationsMultimap.get(key).endRedoOp(OpType.Get, srd, path);
    }

    @Override
    public void putEnd(ByteArray key, SRD srd, BranchPath path) {
        keyOperationsMultimap.get(key).endRedoOp(OpType.Put, srd, path);
    }

    @Override
    public void deleteEnd(ByteArray key, SRD srd, BranchPath path) {
        keyOperationsMultimap.get(key).endRedoOp(OpType.Delete, srd, path);
    }

    @Override
    void getVersionEnd(ByteArray key, SRD srd, BranchPath path) {
        keyOperationsMultimap.get(key).endRedoOp(OpType.GetVersion, srd, path);
    }

    @Override
    public StsBranchPair getVersionStart(ByteArray key, SRD srd, BranchPath path) {
        return keyOperationsMultimap.get(key).redoRead(OpType.GetVersion, srd, path);

    }

    public boolean ignore(ByteArray key, SRD srd, BranchPath path) {
        return keyOperationsMultimap.get(key).ignore(srd, path);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        RedoScheduler other = (RedoScheduler) obj;
        if(keyOperationsMultimap == null) {
            if(other.keyOperationsMultimap != null)
                return false;
        } else if(!keyOperationsMultimap.equals(other.keyOperationsMultimap))
            return false;
        return true;
    }

}
