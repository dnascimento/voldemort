/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.RUD;
import voldemort.undoTracker.branching.BranchPath;
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
        return archive.get(key).redoWrite(rud, path);
    }

    /*
     * writes are perform in new branch
     */
    @Override
    public StsBranchPair deleteStart(ByteArray key, RUD rud, BranchPath path) {
        return archive.get(key).redoWrite(rud, path);
    }

    @Override
    public void getEnd(ByteArray key) {
        archive.get(key).endRedoRead();
    }

    @Override
    public void putEnd(ByteArray key) {
        archive.get(key).endRedoWrite();
    }

    @Override
    public void deleteEnd(ByteArray key) {
        archive.get(key).endRedoWrite();
    }

    @Override
    public StsBranchPair getVersionStart(ByteArray clone, RUD rud, BranchPath path) {
        return new StsBranchPair(path.current.sts, rud.branch);
    }

    public boolean unlock(ByteArray key, RUD rud, long redoCommit) {
        return archive.get(key).unlockOp(rud, redoCommit);
    }
}
