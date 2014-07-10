/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.RUD;
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
public class RedoScheduler extends AccessSchedule {

    OpMultimap archive;

    public RedoScheduler(OpMultimap archive) {
        super();
        this.archive = archive;
    }

    /*
     * reads are perform in new branch or the base commit branch
     */
    @Override
    public StsBranchPair getStart(ByteArray key, RUD rud, BranchPath path) {
        return archive.get(key).redoRead(rud, path);
    }

    /*
     * writes are perform in new branch
     */
    @Override
    public StsBranchPair putStart(ByteArray key, RUD rud, BranchPath path) {
        return archive.get(key).redoWrite(OpType.Put, rud, path);
    }

    /*
     * writes are perform in new branch
     */
    @Override
    public StsBranchPair deleteStart(ByteArray key, RUD rud, BranchPath path) {
        return archive.get(key).redoWrite(OpType.Delete, rud, path);
    }

    @Override
    public void getEnd(ByteArray key, RUD rud, BranchPath path) {
        archive.get(key).endRedoOp(OpType.Get, rud, path);
    }

    @Override
    public void putEnd(ByteArray key, RUD rud, BranchPath path) {
        archive.get(key).endRedoOp(OpType.Put, rud, path);
    }

    @Override
    public void deleteEnd(ByteArray key, RUD rud, BranchPath path) {
        archive.get(key).endRedoOp(OpType.Delete, rud, path);
    }

    @Override
    public StsBranchPair getVersionStart(ByteArray clone, RUD rud, BranchPath path) {
        return new StsBranchPair(path.current.sts, rud.branch);
    }

    public boolean ignore(ByteArray key, RUD rud, BranchPath path) {
        return archive.get(key).ignore(rud, path);
    }
}
