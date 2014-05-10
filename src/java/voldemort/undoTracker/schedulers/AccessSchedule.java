/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import voldemort.VoldemortException;
import voldemort.undoTracker.RUD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

public abstract class AccessSchedule {

    abstract StsBranchPair getStart(ByteArray key, RUD rud, BranchPath current);

    abstract StsBranchPair putStart(ByteArray key, RUD rud, BranchPath path);

    abstract StsBranchPair deleteStart(ByteArray key, RUD rud, BranchPath path);

    abstract StsBranchPair getVersionStart(ByteArray clone, RUD rud, BranchPath path);

    abstract void getEnd(ByteArray key);

    abstract void putEnd(ByteArray key);

    abstract void deleteEnd(ByteArray key);

    public void opEnd(OpType op, ByteArray key) {
        switch(op) {
            case Delete:
                deleteEnd(key);
                break;
            case Get:
                getEnd(key);
                break;
            case Put:
                putEnd(key);
                break;
            default:
                throw new VoldemortException("Unknown operation");
        }
    }

    public StsBranchPair opStart(OpType op, ByteArray key, RUD rud, BranchPath path) {
        switch(op) {
            case Delete:
                return deleteStart(key, rud, path);
            case Get:
                return getStart(key, rud, path);
            case Put:
                return putStart(key, rud, path);
            case GetVersion:
                return getVersionStart(key, rud, path);
            default:
                throw new VoldemortException("Unknown operation");
        }
    }

}
