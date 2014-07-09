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
import voldemort.undoTracker.RUD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.commits.CommitList;

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

    private List<Long> threadsToWake = new ArrayList<Long>();

    /**
     * Keep track of last sent dependency to the manager
     */
    private int sentDependency = 0;

    /**
     * Committed versions management
     */
    private CommitList commitList = new CommitList();

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
    public boolean hasLocked() {
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
            // exception: the first elements of list are get ops because
            // someone
            // tried to get the entry before it exists
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

    /******************************************************************************************************/

    /**
     * Voldemort client gets the version of current entry before PUT. The method
     * returns the correct branch to use. The client can query the branch to
     * know the version
     * 
     * @param rud
     * @param sts
     * @return
     */
    public synchronized StsBranchPair getVersionToPut(RUD rud, BranchPath path) {
        if(rud.rid < path.current.sts) {
            // write only over old values
            return commitList.getBiggestSmallerCommit(path, rud.rid);
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
    public synchronized StsBranchPair trackReadAccess(RUD rud, BranchPath path) {
        Op op = new Op(rud.rid, OpType.Get);
        list.add(op);

        /* If rid < commit timestamp, read only old values */
        if(rud.rid < path.current.sts) {
            // read only old values
            return commitList.getBiggestSmallerCommit(path, rud.rid);
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
    public synchronized StsBranchPair trackWriteAccess(OpType type, RUD rud, BranchPath path) {
        Op op = new Op(rud.rid, type);
        list.add(op);

        /* If rid < commit timestamp, write only old values */
        if(rud.rid < path.current.sts) {
            // write only old values
            return commitList.getBiggestSmallerCommit(path, rud.rid);
        } else {
            StsBranchPair latest = commitList.getLatest(path);
            // write in the current commit and branch
            if(path.current.sts > latest.sts) {
                // new commit
                return commitList.addNewCommit(path.current.sts, rud.branch);
            }
            return latest;
        }
    }

    /*------------------------------ REDO ------------------------------- */

    public synchronized boolean isModified(short redoBranch) {
        if(getRedoIterator() != null && getRedoIterator().getBranch() == redoBranch)
            return true;
        return false;
    }

    /**
     * To be the next operation, it must exist in table. Otherwise, it add
     * itself to the table and waits.
     * 
     * @param rud
     */
    private synchronized void isNextOp(RUD rud, long baseCommit) {

        log.info("isNextOp " + rud.rid);
        Op next = getOrNewRedoIterator(rud.branch, baseCommit).next();
        if(next == null) {
            log.error("Next is null but there is an operation remaining");
            throw new VoldemortException("Next is null but there is an operation remaining");
        }
        if(next.rid != rud.rid) {
            while(!threadsToWake.contains(rud.rid)) {
                log.info("Operation locked " + rud.rid + " waiting for: " + next.rid);
                try {
                    this.wait();
                } catch(InterruptedException e) {
                    log.error(e);
                }
                log.info("Trying to be unlocked " + rud.rid);
            }
            threadsToWake.remove(rud.rid);
        }
        log.info("free to go: " + rud.rid);
    }

    /**
     * Current is read. If next is write, then wake
     * 
     * @param type
     */
    public synchronized void endRedoRead() {
        RedoIterator i = getRedoIterator();
        if(i.hasNext()) {
            Op next = i.next();
            threadsToWake.add(next.rid);
            this.notifyAll();
        }
    }

    /**
     * Current is write. Wake the next reads or THE next write
     * 
     * @param type
     */
    public synchronized void endRedoWrite() {
        RedoIterator it = getRedoIterator();

        if(it.hasNext()) {
            Op next = it.next();
            threadsToWake.add(next.rid);
            if(next.type.equals(OpType.Get)) {
                int i = 1;
                while(it.hasRead(i)) {
                    threadsToWake.add(it.peakRead().rid);
                    i++;
                }
            }
        }
        this.notifyAll();
    }

    /**
     * Simulate the end of the original action to unlock the remain actions
     * 
     * @param rud
     * @return was it locking other actions?
     */
    public synchronized boolean unlockOp(RUD rud, long redoRid) {
        RedoIterator it = getOrNewRedoIterator(rud.branch, redoRid);
        boolean wasNext = it.unlock(rud.rid);
        if(wasNext) {
            threadsToWake.add(it.next().rid);
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
     * @param rud
     * @param sts: branch base Commit
     * @return
     */
    public StsBranchPair redoRead(RUD rud, BranchPath path) {
        if(path == null) {
            log.error("Attempt to read using the redo stub without path");
            throw new VoldemortException("New operation on redo");
        }
        // serialize
        isNextOp(rud, path.current.sts);
        return commitList.redoRead(path);
    }

    /**
     * Commit: baseCommit (provided by request)
     * Branch: redo (in request)
     * 
     * @param rud
     * @return
     */
    public StsBranchPair redoWrite(RUD rud, BranchPath path) {
        isNextOp(rud, path.current.sts);
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
}