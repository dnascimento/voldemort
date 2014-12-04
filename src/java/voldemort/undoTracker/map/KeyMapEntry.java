/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import voldemort.undoTracker.branching.BranchPath;
import voldemort.utils.ByteArray;

/**
 * 
 * @author darionascimento
 * 
 */
public class KeyMapEntry implements Serializable {

    private transient static final long serialVersionUID = 1L;
    public transient static final int INIT_ARRAY_SIZE = 16;

    private transient static final Logger log = Logger.getLogger(KeyMap.class.getName());
    public transient static final boolean debugging = log.isInfoEnabled();

    /**
     * Entry RWLock: not related with list operation. It is the value/database
     * operation locker. true if Fair mode is enable. Check java doc
     */
    public transient ReentrantReadWriteLock valueLocker = new ReentrantReadWriteLock(true);
    private transient ReplayIterator iterator = null;

    /**
     * List of interactions
     */
    public ArrayList<Op> operationList = new ArrayList<Op>(INIT_ARRAY_SIZE);

    /**
     * List of versions in which the data item has been written
     */
    public VersionList versionList = new VersionList();

    /**
     * Keep track of last sent dependency to the manager
     */
    private int sentDependency = 0;

    /**
     * Key of the data item of this entry, for debug proposes
     */
    public ByteArray key;

    public KeyMapEntry(ByteArray key) {
        super();
        this.key = key;
    }

    /**
     * Extract the set of new operations performed in this entry
     * 
     * @param dependenciesCollector dependency between request ids
     * @return the number of new operations
     * @throws Exception
     */
    public synchronized int updateDependencies(UpdateDependenciesMap dependenciesCollector) {
        // TODO no need for synchronized, it just accesses the list
        int lastSentDependency = sentDependency;

        // if empty list or updated, return
        if(sentDependency == (operationList.size() - 1) || operationList.size() == 0) {
            return 0;
        }
        Op lastWrite = null;

        // get last write
        for(int i = sentDependency; i >= 0; i--) {
            if(!operationList.get(i).isGet()) {
                lastWrite = operationList.get(i);
                break;
            }
        }

        if(lastWrite == null) {
            // the first elements of list are get ops, which tried to get the
            // entry before it exists
            while(sentDependency < operationList.size()) {
                if(!operationList.get(sentDependency).isGet()) {
                    lastWrite = operationList.get(sentDependency);
                    break;
                } else {
                    sentDependency++;
                }
            }
        }

        if(lastWrite == null) {
            // no write operations yet
            sentDependency = Math.max(sentDependency - 1, 0);
            return 0;
        }

        // Allocate space for the new operations
        int remaining = operationList.size() - (sentDependency + 1);
        dependenciesCollector.prepareNewBatch(remaining, lastWrite);

        // Add the operations
        for(int i = sentDependency + 1; i < operationList.size(); i++) {
            Op op = operationList.get(i);
            dependenciesCollector.putNew(op);
        }

        sentDependency = operationList.size() - 1;
        return lastSentDependency - sentDependency;
    }

    /**
     * Get a copy of the operation list with RID > baseRid
     * 
     * @param baseRid
     * @return
     */
    public synchronized ArrayList<Op> getOperationsAfterTheRid(long baseRid) {
        ArrayList<Op> copy = new ArrayList<Op>(operationList.size());

        for(Op op: operationList) {
            if(op.rid > baseRid) {
                copy.add(op);
            }
        }
        return copy;
    }

    /*------------------------------ REDO ------------------------------- */
    /**
     * This entry is replaying in the provided branch? The replay process
     * started on this key?
     * 
     * @param replayBranch
     * @return
     */
    public synchronized boolean isReplayingInBranch(short replayBranch) {
        if(iterator != null && iterator.getBranch() == replayBranch)
            return true;
        return false;
    }

    /**
     * Get replay iterator, creating if needed
     * 
     * @param branch
     * @param baseRid
     * @return
     */
    public ReplayIterator getOrNewReplayIterator(BranchPath branchPath) {
        if(iterator == null || iterator.getBranch() != branchPath.branch) {
            iterator = new ReplayIterator(branchPath, operationList);
        }
        return iterator;
    }

    /* -------------------------- others ---------------------------- */

    @Override
    public synchronized String toString() {
        return "[Entry:" + operationList + ", sentDependency=" + sentDependency + "]";
    }

    @Override
    public synchronized int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((versionList == null) ? 0 : versionList.hashCode());
        result = prime * result + ((operationList == null) ? 0 : operationList.hashCode());
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
        KeyMapEntry other = (KeyMapEntry) obj;
        if(versionList == null) {
            if(other.versionList != null)
                return false;
        } else if(!versionList.equals(other.versionList))
            return false;
        if(operationList == null) {
            if(other.operationList != null)
                return false;
        } else if(!operationList.equals(other.operationList))
            return false;
        if(sentDependency != other.sentDependency)
            return false;
        return true;
    }

    public synchronized int countOperations() {
        return operationList.size();
    }

    public String operationListToString() {
        StringBuilder sb = new StringBuilder();
        for(Op op: operationList) {
            sb.append(op.toString());
            sb.append(" ");
        }
        return sb.toString();
    }

    // public synchronized Op getLastWrite() {
    // for(int i = list.size() - 1; i > 0; i--) {
    // Op op = list.get(i);
    // if(!op.isGet()) {
    // return op;
    // }
    // }
    // return null;
    // }

}
