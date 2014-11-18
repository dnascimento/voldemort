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
import voldemort.utils.ByteArray;

import com.google.protobuf.ByteString;

/**
 * A HashMap where same items are append to a list and each entry has a
 * read/write locker to schedule the operation
 * 
 * @author darionascimento
 * 
 */
public class KeyMap implements Serializable {

    private transient static final long serialVersionUID = 1L;
    private transient static final Logger log = Logger.getLogger(KeyMap.class.getName());

    /**
     * Each entry of this map represents the metadata of a data entry of
     * voldemort
     */
    private ConcurrentHashMap<ByteArray, KeyMapEntry> map = new ConcurrentHashMap<ByteArray, KeyMapEntry>(2000,
                                                                                                          (float) 0.75,
                                                                                                          25);

    /**
     * Get the entry and create if it do not exists
     * 
     * @param key
     * @return
     */
    public KeyMapEntry get(ByteArray key) {
        KeyMapEntry entry = map.get(key);
        if(entry == null) {
            entry = map.putIfAbsent(key, new KeyMapEntry(key));
            log.debug("Creating new entry");
            if(entry == null) {
                entry = map.get(key);
            }
            if(!entry.key.equals(key)) {
                throw new RuntimeException("Get key but entry is different");
            }
        }
        return entry;
    }

    /**
     * Get the operation list of a specific key for selective replay, required
     * by the manager
     * 
     * @param keysList
     * @param baseRid
     * @return
     */
    public HashMap<ByteString, ArrayList<Op>> getOperationList(List<ByteString> keysList,
                                                               long baseRid) {
        HashMap<ByteString, ArrayList<Op>> result = new HashMap<ByteString, ArrayList<Op>>();
        for(ByteString key: keysList) {
            KeyMapEntry entry = get(new ByteArray(key.toByteArray()));
            ArrayList<Op> operations = entry.getOperationsAfterTheRid(baseRid);
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
            KeyMapEntry entry = map.get(key);
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
        KeyMap other = (KeyMap) obj;
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
        for(Entry<ByteArray, KeyMapEntry> entry: map.entrySet()) {
            sb.append(ByteArray.toAscii(entry.getKey()));
            sb.append(entry.getValue().operationListToString() + "\n");
        }
        return sb.toString();
    }

    /**
     * Get last write action in a specific key
     * 
     * @param key
     * @return
     */
    // public Op getLastWrite(ByteArray key) {
    // OpMultimapEntry l = map.get(key);
    // Op op = null;
    // if(l != null) {
    // op = l.getLastWrite();
    // }
    // return op;
    // }

}
