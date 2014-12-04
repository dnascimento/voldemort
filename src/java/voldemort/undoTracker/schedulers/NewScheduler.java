/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import java.io.Serializable;

import org.apache.log4j.Logger;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.KeyMap;
import voldemort.undoTracker.map.KeyMapEntry;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.VersionShuttle;
import voldemort.utils.ByteArray;

/**
 * @author darionascimento
 * 
 */
public class NewScheduler extends OperationSchedule implements Serializable {

    private static final long serialVersionUID = 1L;
    private transient static final Logger log = Logger.getLogger(NewScheduler.class.getName());

    private DBProxy dbProxy;

    /**
     * Each request handler has a UndoStub instance
     * 
     * @param dbProxy
     */
    public NewScheduler(DBProxy dbProxy, KeyMap keyOperationsMultimap) {
        super(keyOperationsMultimap);
        this.dbProxy = dbProxy;
    }

    /**
     * Get the version to be accessed by the new operation.
     * If the request rid < current snapshot id, then access only old values.
     * else: if read, access the latest version available. If write, create a
     * new version if necessary.
     */
    @Override
    public VersionShuttle startOperation(OpType type, ByteArray key, SRD srd, BranchPath path) {
        // Wait if restraining
        if(srd.restrain) {
            // new request but may need to wait to avoid dirty reads
            // OpMultimapEntry l = keyOperationsMultimap.get(key);
            // if(l.isReplayingInBranch(path.current.branch)) {
            path = dbProxy.restrain(srd.branch);
            srd.branch = path.branch; // update the branchPath
            // }
        }

        KeyMapEntry entry = keyOperationsMultimap.get(key);

        // Gain access to the read/write lock to serialize the accesses
        if(type.isWrite()) {
            entry.valueLocker.writeLock().lock();
        } else {
            entry.valueLocker.readLock().lock();
        }

        // Log new incoming operation
        synchronized(entry) {
            Op op = new Op(srd.rid, type);
            entry.operationList.add(op);
            // log.debug("new:" + op + ByteArray.toAscii(key) + srd);

            /* If rid < snapshot timestamp, write only old values */
            if(srd.rid < path.latestVersion.sid) {
                // write only old values
                return entry.versionList.getLastestVersionPreviousToSnapshot(path, srd.rid);
            } else {
                // read the most recent
                VersionShuttle latest = entry.versionList.getLatestVersionInPath(path);
                if(type.isRead()) {
                    return latest;
                }
                // write in the current version
                if(path.latestVersion.sid > latest.sid) {
                    return entry.versionList.addNewVersion(path.latestVersion.sid);
                }
                return latest;
            }
        }
    }

    @Override
    public void endOperation(OpType type, ByteArray key, SRD srd, BranchPath path) {
        KeyMapEntry entry = keyOperationsMultimap.get(key);
        if(type.isWrite()) {
            entry.valueLocker.writeLock().unlock();
        } else {
            entry.valueLocker.readLock().unlock();
        }
    }

}
