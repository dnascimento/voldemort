/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.KeyMap;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.VersionShuttle;
import voldemort.utils.ByteArray;

public abstract class OperationSchedule {

    protected final KeyMap keyOperationsMultimap;

    public OperationSchedule(KeyMap keyOperationsMultimap) {
        this.keyOperationsMultimap = keyOperationsMultimap;
    }

    public abstract VersionShuttle startOperation(OpType type,
                                                  ByteArray key,
                                                  SRD srd,
                                                  BranchPath current);

    public abstract void endOperation(OpType type, ByteArray key, SRD srd, BranchPath path);

}
