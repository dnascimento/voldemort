/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import java.io.Serializable;

import org.apache.log4j.Logger;

import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.KeyMap;
import voldemort.undoTracker.map.KeyMapEntry;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.ReplayIterator;
import voldemort.undoTracker.map.VersionShuttle;
import voldemort.utils.ByteArray;

/**
 * 
 * @author darionascimento
 * 
 */
public class ReplayScheduler extends OperationSchedule implements Serializable {

    private static final long serialVersionUID = 1L;
    private transient static final Logger log = Logger.getLogger(ReplayScheduler.class.getName());

    public ReplayScheduler(KeyMap keyOperationsMultimap) {
        super(keyOperationsMultimap);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        ReplayScheduler other = (ReplayScheduler) obj;
        if(keyOperationsMultimap == null) {
            if(other.keyOperationsMultimap != null)
                return false;
        } else if(!keyOperationsMultimap.equals(other.keyOperationsMultimap))
            return false;
        return true;
    }

    /*
     * reads are perform in new branch or the base snapshot branch.
     * writes are perform in new branch
     * To be the next operation, it must exist in table. Otherwise, it add
     * itself to the table and waits.
     */
    @Override
    public VersionShuttle startOperation(OpType type, ByteArray key, SRD srd, BranchPath path) {
        Op op = new Op(srd.rid, type);
        KeyMapEntry entry = keyOperationsMultimap.get(key);

        long baseSnapshot = path.current.sid;

        synchronized(entry) {
            // log.debug("replay:" + op + ByteArray.toAscii(key) + srd);
            ReplayIterator it = entry.getOrNewReplayIterator(srd.branch, baseSnapshot);
            while(!it.operationIsAllowed(op, key)) {
                try {
                    this.wait();
                } catch(InterruptedException e) {
                    log.error(e);
                }
                log.debug("Operation blocked: " + srd.rid);
                // Trying to be unlocked: srd.rid
            }
            // log.debug("replay end:" + op + ByteArray.toAscii(key) + srd);
            // free to go srd.rid
        }

        if(op.isWrite()) {
            return entry.versionList.replayWrite(path);
        } else {
            return entry.versionList.replayRead(path);
        }
    }

    /**
     * Operation is done
     */
    @Override
    public void endOperation(OpType type, ByteArray key, SRD srd, BranchPath path) {
        KeyMapEntry entry = keyOperationsMultimap.get(key);

        synchronized(entry) {
            ReplayIterator it = entry.getOrNewReplayIterator(srd.branch, path.current.sid);
            Op op = new Op(srd.rid, type);
            // log.error(ByteArray.toAscii(key) + ": end: " + type + " " +
            // srd.rid);

            if(it.endOp(op)) {
                this.notifyAll();
            }
        }
    }

    /**
     * Simulate the end of the original action to unlock the remain actions
     * 
     * @param srd
     * @return true if there is any operation locked waiting for this operation
     */
    public void ignore(ByteArray key, SRD srd, BranchPath path) {
        KeyMapEntry entry = keyOperationsMultimap.get(key);

        synchronized(entry) {
            ReplayIterator it = entry.getOrNewReplayIterator(srd.branch, path.current.sid);
            if(it.ignore(srd.rid)) {
                // log.info("Ignore key: " + ByteArray.toAscii(key));
                this.notifyAll();
            }
        }
    }
}
