/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.undoTracker.RUD;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.commits.CommitController;

import com.google.common.collect.HashMultimap;

/**
 * 
 * @author darionascimento
 * 
 */
public class OpMultimapEntry implements Serializable {

    private transient static final Logger log = LogManager.getLogger(OpMultimap.class.getName());

    private enum WaitState {
        Waiting,
        Unlocked;
    }

    private HashMap<Long, WaitState> waitTable = new HashMap<Long, WaitState>();

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Avoid re-size version array, average number of version per key
     */
    public final int INIT_ARRAY_SIZE = 16;

    /**
     * Entry RWLock: not related with list access. It is the value/database
     * access
     * locker.
     */
    private RWLock lock = new RWLock();

    /**
     * Use synchronized(this) to access the list. List of past interactions
     */
    private ArrayList<Op> list = new ArrayList<Op>(INIT_ARRAY_SIZE);

    /**
     * Keep track of next or last write
     */
    private int redoPos = 0;

    /**
     * Keep track of last sent dependency to the manager
     */
    private int sentDependency = 0;

    private CommitController commitList = new CommitController();

    public OpMultimapEntry(ArrayList<Op> list) {
        this.list = list;
    }

    public OpMultimapEntry() {

    }

    public void addAll(List<Op> values) {
        synchronized(this) {
            list.addAll(values);
        }
    }

    public synchronized Op getLastWrite() {
        synchronized(this) {
            for(int i = list.size() - 1; i > 0; i--) {
                Op op = list.get(i);
                if(op.type != OpType.Get) {
                    return op;
                }
            }
            return null;
        }
    }

    public boolean isEmpty() {
        synchronized(this) {
            return list.isEmpty();
        }
    }

    public void addLast(Op op) {
        synchronized(this) {
            list.add(op);
        }
    }

    public ArrayList<Op> getAll() {
        return list;
    }

    public ArrayList<Op> extractAll() {
        synchronized(this) {
            ArrayList<Op> data = list;
            list = new ArrayList<Op>();
            return data;
        }
    }

    /* /////////// Value Access Lock ////////////////////// */

    public void lockWrite() {
        lock.lockWrite();
    }

    public void lockRead() {
        lock.lockRead();
    }

    public void unlockWrite() {
        lock.releaseWrite();
    }

    public void unlockRead() {
        lock.releaseRead();
    }

    /**
     * Count if there are some operation running
     * 
     * @return
     */
    public boolean hasLocked() {
        return lock.hasPendent();
    }

    public boolean updateDependencies(HashMultimap<Long, Long> dependencyPerRid) throws Exception {
        // 1st get previous write
        synchronized(this) {
            synchronized(dependencyPerRid) {
                assert (list.size() > 0);
                if(sentDependency == (list.size() - 1)) {
                    return false;
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
                    throw new Exception("Last write == -1");
                }
                // update new dependencies
                for(int i = sentDependency + 1; i < list.size(); i++) {
                    Op op = list.get(i);
                    if(op.type == Op.OpType.Get) {
                        dependencyPerRid.put(op.rid, lastWrite);
                    } else {
                        lastWrite = op.rid;
                    }
                }
                sentDependency = list.size() - 1;
            }
        }
        return true;
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
    public StsBranchPair getVersionToPut(RUD rud, StsBranchPair current) {
        if(rud.rid < current.sts) {
            // write only over old values
            return commitList.getBiggestSmallerCommit(current);
        } else {
            // write in the new commit
            return commitList.latestVersion(current);
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
    public StsBranchPair trackReadAccess(RUD rud, StsBranchPair current) {
        Op op = new Op(rud.rid, OpType.Get);
        addLast(op);

        /* If rid < commit timestamp, read only old values */
        if(rud.rid < current.sts) {
            // read only old values
            return commitList.getBiggestSmallerCommit(current);
        } else {
            // read the most recent
            return commitList.latestVersion(current);
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
    public StsBranchPair trackWriteAccess(OpType type, RUD rud, StsBranchPair current) {
        Op op = new Op(rud.rid, type);
        addLast(op);

        /* If rid < commit timestamp, write only old values */
        if(rud.rid < current.sts) {
            // write only old values
            return commitList.getBiggestSmallerCommit(current);
        } else {
            StsBranchPair latest = commitList.latestVersion(current);
            // write in the current commit and branch
            if(current.sts > latest.sts) {
                // new commit
                commitList.addNewCommit(current.sts, rud.branch);
            }
            return latest;
        }
    }

    /*------------------------------ REDO ------------------------------- */
    /**
     * Select the first action after the commit
     * 
     * @param rid
     */
    private void startRedo(long commitRid, short branch) {
        newBranch(branch);
        // TODO repensar
        for(redoPos = 0; redoPos < list.size(); redoPos++) {
            if(list.get(redoPos).rid >= commitRid)
                break;
        }
        if(redoPos == list.size())
            return;

        Op next = list.get(redoPos);
        // wake the next write or next readS
        if(!next.type.equals(OpType.Get)) {
            wakeOrAdd(next.rid, next.type);
        } else {
            int i = 0;
            do {
                next = list.get(redoPos + i);
                if(!next.type.equals(OpType.Get))
                    break;
                i++;
                wakeOrAdd(next.rid, next.type);
            } while((redoPos + i) < list.size());
        }
    }

    public boolean isModified() {
        // TODO improve to support redo
        synchronized(this) {
            return (redoPos == 0);
        }
    }

    /**
     * To be the next op, it must exist in table. Otherwise, it add itself to
     * the table and waits.
     * 
     * @param rud
     */
    public void isNextOp(RUD rud) {
        synchronized(waitTable) {
            WaitState s = waitTable.remove(rud.rid);
            if(s == null) {
                WaitState obj = WaitState.Waiting;
                waitTable.put(rud.rid, obj);
                try {
                    obj.wait();
                } catch(InterruptedException e) {
                    log.error(e);
                }
                s = waitTable.remove(rud.rid);
            }
        }
    }

    /**
     * 
     * @param type
     */
    public void endOp(OpType type) {
        synchronized(this) {
            ++redoPos;

            if(redoPos == list.size())
                return;

            Op next = list.get(redoPos);

            // If current is read and next is write:
            if(type.equals(OpType.Get)) {
                if(!next.type.equals(OpType.Get)) {
                    wakeOrAdd(next.rid, next.type);
                }
            } else {
                // if write, wake next reads or THE next write
                if(next.type.equals(OpType.Get)) {
                    int i = 0;
                    do {
                        next = list.get(redoPos + i);
                        if(!next.type.equals(OpType.Get))
                            break;
                        i++;
                        wakeOrAdd(next.rid, next.type);
                    } while((redoPos + i) < list.size());
                } else {
                    wakeOrAdd(next.rid, next.type);
                }
            }
        }
    }

    private void wakeOrAdd(long rid, OpType type) {
        synchronized(waitTable) {
            WaitState state = waitTable.remove(rid);
            if(state.equals(WaitState.Unlocked)) {
                endOp(type);
            } else {
                state.notifyAll();
            }
        }
    }

    /**
     * Simulate the end of the original action to unlock the remain actions
     * 
     * @param rud
     * @return was it locking other actions?
     */
    public boolean unlockOp(RUD rud) {
        WaitState s = waitTable.remove(rud.rid);
        if(s == null) {
            waitTable.put(rud.rid, WaitState.Unlocked);
            return false;
        } else {
            OpType type = searchType(rud.rid);
            endOp(type);
            return true;
        }
    }

    private OpType searchType(long rid) {
        for(int i = redoPos; i < list.size(); i++) {
            if(list.get(i).rid == rid) {
                return list.get(i).type;
            }
        }
        log.error("SEARCHTYPE FAILT!!!!");
        throw new VoldemortException("SEARCHTYPE FAILT!!!!");
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
    public StsBranchPair redoRead(RUD rud, short redoBranch, long redoBaseCommit) {
        // serialize
        isNextOp(rud);
        return commitList.redoRead(redoBranch, redoBaseCommit);
    }

    /**
     * Commit: baseCommit (provided by request)
     * Branch: redo (in request)
     * 
     * @param rud
     * @return
     */
    public StsBranchPair redoWrite(RUD rud, long sts) {
        isNextOp(rud);
        return commitList.addRedoVersion(sts, rud.branch);
    }

    /* -------------------------- others ---------------------------- */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        synchronized(this) {
            if(this == obj)
                return true;
            if(obj == null)
                return false;
            if(getClass() != obj.getClass())
                return false;
            OpMultimapEntry other = (OpMultimapEntry) obj;
            if(list == null) {
                if(other.list != null)
                    return false;
            } else if(!list.equals(other.list))
                return false;
            return true;
        }
    }

    @Override
    public String toString() {
        synchronized(this) {

            return "[Entry:" + list + ", redoPos=" + redoPos + ", sentDependency=" + sentDependency
                   + "]";
        }
    }

    public int size() {
        synchronized(this) {
            return list.size();
        }
    }
}
