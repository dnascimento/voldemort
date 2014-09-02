/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import voldemort.VoldemortException;
import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

public abstract class AccessSchedule {

    abstract StsBranchPair getStart(ByteArray key, SRD srd, BranchPath current);

    abstract StsBranchPair putStart(ByteArray key, SRD srd, BranchPath path);

    abstract StsBranchPair deleteStart(ByteArray key, SRD srd, BranchPath path);

    abstract StsBranchPair getVersionStart(ByteArray clone, SRD srd, BranchPath path);

    abstract void getEnd(ByteArray key, SRD srd, BranchPath path);

    abstract void putEnd(ByteArray key, SRD srd, BranchPath path);

    abstract void deleteEnd(ByteArray key, SRD srd, BranchPath path);

    public void opEnd(OpType op, ByteArray key, SRD srd, BranchPath path) {
        switch(op) {
            case Delete:
                deleteEnd(key, srd, path);
                break;
            case Get:
                getEnd(key, srd, path);
                break;
            case Put:
                putEnd(key, srd, path);
                break;
            default:
                throw new VoldemortException("Unknown operation");
        }
    }

    public StsBranchPair opStart(OpType op, ByteArray key, SRD srd, BranchPath path) {
        switch(op) {
            case Delete:
                return deleteStart(key, srd, path);
            case Get:
                return getStart(key, srd, path);
            case Put:
                return putStart(key, srd, path);
            case GetVersion:
                return getVersionStart(key, srd, path);
            default:
                throw new VoldemortException("Unknown operation");
        }
    }

}
