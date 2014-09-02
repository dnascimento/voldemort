/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.commits.CommitList;
import voldemort.utils.ByteArray;

import com.google.common.collect.HashMultimap;

/**
 * 
 * @author darionascimento
 * 
 */
public class OpMultimapEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient static final Logger log = Logger.getLogger(OpMultimap.class.getName());

    /**
     * Avoid re-size version array, average number of version per key
     */
    public final int INIT_ARRAY_SIZE = 16;

    /**
     * Entry RWLock: not related with list access. It is the value/database
     * access locker.
     */
    private transient RWLock valueLocker = new RWLock();

    private transient RedoIterator iterator = null;

    /**
     * List of interactions
     */
    private ArrayList<Op> list = new ArrayList<Op>(INIT_ARRAY_SIZE);

    /**
     * Keep track of last sent dependency to the manager
     */
    private int sentDependency = 0;

    /**
     * Committed versions management
     */
    private CommitList commitList = new CommitList();

    private ByteArray key;

    public OpMultimapEntry(ByteArray key) {
        super();
        this.key = key;
    }

    public synchronized void addAll(List<Op> values) {
        list.addAll(values);
    }

    public synchronized Op getLastWrite() {
        for(int i = list.size() - 1; i > 0; i--) {
            Op op = list.get(i);
            if(op.type != OpType.Get) {
                return op;
            }
        }
        return null;
    }

    public synchronized boolean isEmpty() {
        return list.isEmpty();
    }

    public synchronized ArrayList<Op> getAll() {
        return list;
    }

    public synchronized ArrayList<Op> extractAll() {
        ArrayList<Op> data = list;
        list = new ArrayList<Op>();
        return data;
    }

    /* /////////// Value Access Lock ////////////////////// */

    public void lockWrite() {
        valueLocker.lockWrite();
    }

    public void lockRead() {
        valueLocker.lockRead();
    }

    public void unlockWrite() {
        valueLocker.releaseWrite();
    }

    public void unlockRead() {
        valueLocker.releaseRead();
    }

    /**
     * Count if there are some operation running
     * 
     */
    public boolean isLocked() {
        return valueLocker.hasPendent();
    }

    /**
     * Extract the set of new operations performed in this entry
     * 
     * @param dependencyMap dependency between request ids
     * @return the number of new operations
     * @throws Exception
     */
    public synchronized int updateDependencies(HashMultimap<Long, Long> dependencyMap) {
        int lastSentDependency = sentDependency;

        // if empty list or updated, return
        if(sentDependency == (list.size() - 1) || list.size() == 0) {
            return 0;
        }
        long lastWrite = -1;

        // get last write
        for(int i = sentDependency; i >= 0; i--) {
            if(list.get(i).type != OpType.Get) {
                lastWrite = list.get(i).rid;
                break;
            }
        }

        if(lastWrite == -1) {
            // the first elements of list are get ops, which tried to get the
            // entry before it exists
            while(sentDependency < list.size()) {
                if(list.get(sentDependency).type != OpType.Get) {
                    lastWrite = list.get(sentDependency).rid;
                    break;
                } else {
                    sentDependency++;
                }
            }
        }

        if(lastWrite == -1) {
            // no write operations yet
            sentDependency = Math.max(sentDependency - 1, 0);
            return 0;
        }

        // update new dependencies
        for(int i = sentDependency + 1; i < list.size(); i++) {
            Op op = list.get(i);
            if(op.type == Op.OpType.Get) {
                dependencyMap.put(op.rid, lastWrite);
            } else {
                lastWrite = op.rid;
            }
        }
        sentDependency = list.size() - 1;
        return lastSentDependency - sentDependency;
    }

    /**
     * Get a copy of the access list
     * 
     * @param baseRid
     * @return
     */
    public synchronized ArrayList<Op> getAccesses(long baseRid) {
        ArrayList<Op> copy = new ArrayList<Op>(list.size());

        for(Op op: list) {
            if(op.rid < baseRid) {
                copy.add(op);
            }
        }
        return copy;
    }

    /******************************************************************************************************/

    /**
     * Voldemort client gets the version of current entry before PUT. The method
     * returns the correct branch to use. The client can query the branch to
     * know the version
     * 
     * @param srd
     * @param sts
     * @return
     */
    public synchronized StsBranchPair getVersionToPut(SRD srd, BranchPath path) {
        if(srd.rid < path.current.sts) {
            // write only over old values
            return commitList.getBiggestSmallerCommit(path, srd.rid);
        } else {
            // write in the new commit
            return commitList.getLatest(path);
        }
    }

    /**
     * Choose the correct commit to access in reads and which will write:
     * Old commit: read/write the most recent but older
     * New/Current: read the most recent, write the actual
     * 
     * @param type
     * @param rid
     * @param sts
     * @return
     */
    public synchronized StsBranchPair trackReadAccess(SRD srd, BranchPath path) {
        Op op = new Op(srd.rid, OpType.Get);
        list.add(op);

        /* If rid < commit timestamp, read only old values */
        if(srd.rid < path.current.sts) {
            // read only old values
            return commitList.getBiggestSmallerCommit(path, srd.rid);
        } else {
            // read the most recent
            return commitList.getLatest(path);
        }
    }

    /**
     * Choose the correct commit to access in reads and which will write:
     * Old commit: read/write the most recent but older
     * New/Current: read the most recent, write the actual
     * 
     * @param type
     * @param rid
     * @param sts
     * @return
     */
    public synchronized StsBranchPair trackWriteAccess(OpType type, SRD srd, BranchPath path) {
        Op op = new Op(srd.rid, type);
        list.add(op);

        /* If rid < commit timestamp, write only old values */
        if(srd.rid < path.current.sts) {
            // write only old values
            return commitList.getBiggestSmallerCommit(path, srd.rid);
        } else {
            StsBranchPair latest = commitList.getLatest(path);
            // write in the current commit and branch
            if(path.current.sts > latest.sts) {
                // new commit
                return commitList.addNewCommit(path.current.sts, srd.branch);
            }
            return latest;
        }
    }

    /*------------------------------ REDO ------------------------------- */
    /**
     * This entry is replaying in the provided branch? The replay process
     * started on this key?
     * 
     * @param redoBranch
     * @return
     */
    public synchronized boolean isReplayingInBranch(short redoBranch) {
        if(getRedoIterator() != null && getRedoIterator().getBranch() == redoBranch)
            return true;
        return false;
    }

    /**
     * To be the next operation, it must exist in table. Otherwise, it add
     * itself to the table and waits.
     * 
     * @param srd
     */
    private synchronized void startRedoOp(Op op, SRD srd, long baseCommit) {
        log.info("isNextOp " + srd.rid);
        RedoIterator it = getOrNewRedoIterator(srd.branch, baseCommit);
        while(!it.operationIsAllowed(op)) {
            log.info("Operation locked " + srd.rid + " on key: " + DBProxy.hexStringToAscii(key));
            log.info(it.toString());
            try {
                this.wait();
            } catch(InterruptedException e) {
                log.error(e);
            }
            log.info("Trying to be unlocked " + srd.rid);
        }
        log.info("free to go: " + srd.rid);

    }

    /**
     * Operation is done
     * 
     * @param type
     */
    public synchronized void endRedoOp(OpType type, SRD srd, BranchPath path) {
        RedoIterator it = getOrNewRedoIterator(srd.branch, path.current.sts);
        log.info("Op end: " + type + " " + srd.rid);
        Op op = new Op(srd.rid, type);
        if(it.endOp(op)) {
            log.info("Waking the threads of key: " + DBProxy.hexStringToAscii(key));
            this.notifyAll();
        }
    }

    /**
     * Simulate the end of the original action to unlock the remain actions
     * 
     * @param srd
     * @return was it locking other actions?
     */
    public synchronized boolean ignore(SRD srd, BranchPath path) {
        RedoIterator it = getOrNewRedoIterator(srd.branch, path.current.sts);
        if(it.ignore(srd.rid)) {
            log.info("Waking the threads on key: " + DBProxy.hexStringToAscii(key));
            this.notifyAll();
        }
        return true;
    }

    /**
     * Get redo iterator, creating if needed
     * 
     * @param branch
     * @param baseRid
     * @return
     */
    private RedoIterator getOrNewRedoIterator(short branch, long baseRid) {
        if(iterator == null || iterator.getBranch() != branch) {
            iterator = new RedoIterator(branch, baseRid, list);
        }
        return iterator;
    }

    private RedoIterator getRedoIterator() {
        return iterator;
    }

    // ///////////// REDO CODE ////////////////////////////////
    /**
     * Wait until be the next operation
     * Commit: redoBaseCommit
     * Branch: redoBranch or the baseCommit's branch
     * 
     * @param srd
     * @param sts: branch base Commit
     * @return
     */
    public StsBranchPair redoRead(SRD srd, BranchPath path) {
        if(path == null) {
            log.error("Attempt to read using the redo stub without path");
            throw new VoldemortException("New operation on redo");
        }
        // serialize
        startRedoOp(new Op(srd.rid, OpType.Get), srd, path.current.sts);
        return commitList.redoRead(path);
    }

    /**
     * Commit: baseCommit (provided by request)
     * Branch: redo (in request)
     * 
     * @param srd
     * @return
     */
    public StsBranchPair redoWrite(OpType opType, SRD srd, BranchPath path) {
        startRedoOp(new Op(srd.rid, opType), srd, path.current.sts);
        return commitList.redoWrite(path);
    }

    /* -------------------------- others ---------------------------- */

    @Override
    public synchronized String toString() {
        return "[Entry:" + list + ", sentDependency=" + sentDependency + "]";
    }

    @Override
    public synchronized int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + INIT_ARRAY_SIZE;
        result = prime * result + ((commitList == null) ? 0 : commitList.hashCode());
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        result = prime * result + sentDependency;
        return result;
    }

    @Override
    public synchronized boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        OpMultimapEntry other = (OpMultimapEntry) obj;
        if(INIT_ARRAY_SIZE != other.INIT_ARRAY_SIZE)
            return false;
        if(commitList == null) {
            if(other.commitList != null)
                return false;
        } else if(!commitList.equals(other.commitList))
            return false;
        if(list == null) {
            if(other.list != null)
                return false;
        } else if(!list.equals(other.list))
            return false;
        if(sentDependency != other.sentDependency)
            return false;
        return true;
    }

    public synchronized int size() {
        return list.size();
    }

    public synchronized void addLast(Op op) {
        list.add(op);
    }

    public String debugExecutionList() {
        StringBuilder sb = new StringBuilder();
        for(Op op: list) {
            sb.append(op.toString());
            sb.append(" ");
        }
        return sb.toString();
    }
}