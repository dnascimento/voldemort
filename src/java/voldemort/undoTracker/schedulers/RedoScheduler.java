/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.RUD;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

/**
 * 
 * @author darionascimento
 * 
 */
public class RedoScheduler implements AccessSchedule {

    OpMultimap archive;

    public RedoScheduler(OpMultimap archive) {
        super();
        this.archive = archive;
    }

    /*
     * reads are perform in new branch or the base commit branch
     */
    @Override
    public StsBranchPair getStart(ByteArray key, RUD rud, StsBranchPair redoBase) {
        return archive.get(key).redoRead(rud.branch, redoBase.sts);
    }

    /*
     * writes are perform in new branch
     */
    @Override
    public StsBranchPair putStart(ByteArray key, RUD rud, StsBranchPair sts) {
        return archive.get(key).redoWrite(rud);
    }

    /*
     * writes are perform in new branch
     */
    @Override
    public StsBranchPair deleteStart(ByteArray key, RUD rud, StsBranchPair sts) {
        return archive.get(key).redoWrite(rud);
    }

    @Override
    public void getEnd(ByteArray key, RUD rud) {
        archive.get(key).endOp(OpType.Get);
    }

    @Override
    public void putEnd(ByteArray key, RUD rud) {
        archive.get(key).endOp(OpType.Put);
    }

    @Override
    public void deleteEnd(ByteArray key, RUD rud) {
        archive.get(key).endOp(OpType.Delete);
    }

    @Override
    public StsBranchPair getVersionStart(ByteArray clone, RUD rud, StsBranchPair sts) {
        // TODO check
        return new StsBranchPair(sts, rud.branch);
    }

    public boolean unlock(ByteArray key, RUD rud) {
        return archive.get(key).unlockOp(rud);
    }
}
