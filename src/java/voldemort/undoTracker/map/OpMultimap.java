/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */
package voldemort.undoTracker.map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.SRD;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.utils.ByteArray;

import com.google.protobuf.ByteString;

/**
 * A HashMap where same items are append to a list and each entry has a
 * read/write locker to schedule the access
 * 
 * @author darionascimento
 * 
 */
public class OpMultimap implements Serializable {

    private transient static final long serialVersionUID = 1L;
    private transient static final Logger log = Logger.getLogger(OpMultimap.class.getName());

    /**
     * Each entry of this map represents the metadata of a data entry of
     * voldemort
     */
    private ConcurrentHashMap<ByteArray, OpMultimapEntry> map = new ConcurrentHashMap<ByteArray, OpMultimapEntry>(2000,
                                                                                                                  (float) 0.75,
                                                                                                                  25);

    // //////////// Access Control ////////
    /**
     * Access tracking
     * 
     * @param key
     * @param type
     * @param srd
     * @param current
     * @return
     */
    public StsBranchPair trackReadAccess(ByteArray key, SRD srd, BranchPath current) {
        OpMultimapEntry entry = get(key);
        entry.lockRead();
        return entry.trackReadAccess(srd, current);
    }

    public StsBranchPair trackWriteAccess(ByteArray key, OpType writeType, SRD srd, BranchPath path) {
        OpMultimapEntry entry = get(key);
        entry.lockWrite();
        return entry.trackWriteAccess(writeType, srd, path);
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

    public StsBranchPair getVersionToPut(ByteArray key, SRD srd, BranchPath current) {
        OpMultimapEntry entry = get(key);
        entry.lockRead();
        return entry.getVersionToPut(srd, current);
    }

    // // Map Management ////

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
     * Get the entry and create if it do not exists
     * 
     * @param key
     * @return
     */
    public OpMultimapEntry get(ByteArray key) {
        OpMultimapEntry entry = map.get(key);
        if(entry == null) {
            entry = map.putIfAbsent(key, new OpMultimapEntry(key));
            log.debug("Creating new entry");
            if(entry == null) {
                entry = map.get(key);
            }
            if(!entry.getKey().equals(key)) {
                throw new RuntimeException("Get key but entry is different");
            }
        }
        return entry;
    }

    /**
     * Get the access list of a specific key for selective replay, required by
     * the manager
     * 
     * @param keysList
     * @param baseRid
     * @return
     */
    public HashMap<ByteString, ArrayList<Op>> getAccessList(List<ByteString> keysList, long baseRid) {
        HashMap<ByteString, ArrayList<Op>> result = new HashMap<ByteString, ArrayList<Op>>();
        for(ByteString key: keysList) {
            OpMultimapEntry entry = get(new ByteArray(key.toByteArray()));
            ArrayList<Op> operations = entry.getAccesses(baseRid);
            result.put(key, operations);
        }
        return result;
    }

    public Enumeration<ByteArray> getKeySet() {
        return map.keys();
        // TODO this may cause issues, then use keySet which is weakly
        // consistent
    }

    /**
     * Extracts the dependencies of each entry
     * 
     * @param dependenciesPerRid
     * @return
     */
    public boolean updateDependencies(UpdateDependenciesMap dependenciesPerRid) {
        int newDeps = 0;
        int verified = 0;
        Enumeration<ByteArray> keySet = getKeySet();
        while(keySet.hasMoreElements()) {
            ByteArray key = keySet.nextElement();
            OpMultimapEntry entry = map.get(key);
            try {
                newDeps += entry.updateDependencies(dependenciesPerRid);
            } catch(Exception e) {
                log.error(DBProxy.hexStringToAscii(key), e);
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
            sb.append(ByteArray.toAscii(entry.getKey()));
            sb.append(entry.getValue().debugExecutionList() + "\n");
        }
        return sb.toString();
    }

}
