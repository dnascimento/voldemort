/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */
package voldemort.undoTracker.map;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import voldemort.undoTracker.DBUndoStub;
import voldemort.undoTracker.RUD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.utils.ByteArray;

import com.google.common.collect.HashMultimap;

/**
 * A HashMap where same items are append to a list and each entry has a
 * read/write locker to schedule the access
 * 
 * @author darionascimento
 * 
 */
public class OpMultimap implements Serializable {

    private static final long serialVersionUID = 1L;
    private transient static final Logger log = Logger.getLogger(OpMultimap.class.getName());

    /**
     * Each entry of this map represents the metadata of a data entry of
     * voldemort
     */
    private ConcurrentHashMap<ByteArray, OpMultimapEntry> map = new ConcurrentHashMap<ByteArray, OpMultimapEntry>();

    // //////////// Access Control ////////
    /**
     * Access tracking
     * 
     * @param key
     * @param type
     * @param rud
     * @param current
     * @return
     */
    public StsBranchPair trackReadAccess(ByteArray key, RUD rud, BranchPath current) {
        OpMultimapEntry entry = get(key);
        entry.lockRead();
        return entry.trackReadAccess(rud, current);
    }

    public StsBranchPair trackWriteAccess(ByteArray key, OpType writeType, RUD rud, BranchPath path) {
        OpMultimapEntry entry = get(key);
        entry.lockWrite();
        return entry.trackWriteAccess(writeType, rud, path);
    }

    public void endReadAccess(ByteArray key) {
        OpMultimapEntry l = get(key);
        assert (l != null);
        l.unlockRead();
    }

    public void endWriteAccess(ByteArray key) {
        OpMultimapEntry l = get(key);
        assert (l != null);
        l.unlockWrite();
    }

    public StsBranchPair getVersionToPut(ByteArray key, RUD rud, BranchPath current) {
        OpMultimapEntry entry = get(key);
        return entry.getVersionToPut(rud, current);
    }

    // // Map Management ////

    /**
     * Add a set of operations to historic
     * 
     * @param key
     * @param values
     */
    public void putAll(ByteArray key, List<Op> values) {
        log.info("PutAll");
        OpMultimapEntry entry = get(key);
        entry.addAll(values);
    }

    /**
     * Get last write action in a specific key
     * 
     * @param key
     * @return
     */
    public Op getLastWrite(ByteArray key) {
        OpMultimapEntry l = map.get(key);
        Op op = null;
        if(l != null) {
            op = l.getLastWrite();
        }
        return op;
    }

    /**
     * Add new entry to map
     * 
     * @param key
     * @param op
     * @return
     */
    public OpMultimapEntry put(ByteArray key, Op op) {
        OpMultimapEntry l = get(key);
        assert (l != null);
        l.addLast(op);
        return l;
    }

    /**
     * Get the entry and create if it do not exists
     * 
     * @param key
     * @return
     */
    public OpMultimapEntry get(ByteArray key) {
        OpMultimapEntry entry = map.get(key);
        if(entry == null) {
            entry = map.putIfAbsent(key, new OpMultimapEntry(key));
            System.out.println("Creating new entry");
            if(entry == null) {
                entry = map.get(key);
            }
        }
        return entry;
    }

    public Enumeration<ByteArray> getKeySet() {
        return map.keys();
        // TODO this may cause issues, then use keySet which is weakly
        // consistent
        // return map.keySet();
    }

    /**
     * Extracts the dependencies of each entry
     * 
     * @param dependencyPerRid
     * @return
     */
    public boolean updateDependencies(HashMultimap<Long, Long> dependencyPerRid) {
        int newDeps = 0;
        int verified = 0;
        Enumeration<ByteArray> keySet = getKeySet();
        while(keySet.hasMoreElements()) {
            ByteArray key = keySet.nextElement();
            OpMultimapEntry entry = map.get(key);

            assert (entry != null);
            try {
                newDeps += entry.updateDependencies(dependencyPerRid);
            } catch(Exception e) {
                log.error(DBUndoStub.hexStringToAscii(key), e);
            }
            verified++;
        }
        if(newDeps != 0) {
            log.info("Updated " + verified + " dependencies");
        }
        return newDeps != 0;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        OpMultimap other = (OpMultimap) obj;
        if(map == null) {
            if(other.map != null)
                return false;
        } else if(!map.equals(other.map))
            return false;
        return true;
    }

    public int size() {
        return map.size();
    }

    public void clear() {
        map.clear();
    }

    public String debugExecutionList() {
        StringBuilder sb = new StringBuilder();
        for(Entry<ByteArray, OpMultimapEntry> entry: map.entrySet()) {
            sb.append(entry.getValue().debugExecutionList() + "\n");
        }
        return sb.toString();
    }
}
