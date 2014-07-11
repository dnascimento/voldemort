/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker;

import undo.proto.ToManagerProto;
import voldemort.utils.ByteArray;

import com.google.common.collect.ArrayListMultimap;

/**
 * Request Undo Data
 * 
 * @author darionascimento
 * 
 */
public class RUD {

    public enum OpType {
        Put,
        Delete,
        Get;
    }

    public final long rid;
    public short branch;
    public final boolean restrain;
    public ArrayListMultimap<ByteArray, KeyAccess> accessedKeys;
    public final boolean redo;

    public RUD() {
        this(0L, 0, false, false);
    }

    public RUD(long rid) {
        this(rid, 0, false, false);
    }

    public RUD(undo.proto.ToManagerProto.RUD rud) {
        this(rud.getRid(), rud.getBranch(), rud.getRestrain(), rud.getRedo());
    }

    public RUD(long rid, int branch, boolean restrain) {
        this(rid, branch, restrain, false);
    }

    public RUD(long rid, int branch, boolean restrain, boolean redo) {
        this.rid = rid;
        this.branch = (short) branch;
        this.restrain = restrain;
        this.redo = redo;
        this.accessedKeys = null;
    }

    public ToManagerProto.RUD toProto() {
        return ToManagerProto.RUD.newBuilder()
                                 .setBranch(branch)
                                 .setRid(rid)
                                 .setRestrain(restrain)
                                 .build();
    }

    public void addAccessedKey(ByteArray key, String storeName, OpType type) {
        if(accessedKeys == null)
            accessedKeys = ArrayListMultimap.create();
        KeyAccess k = new KeyAccess(storeName, type);
        accessedKeys.put(key, k);
    }

    public ArrayListMultimap<ByteArray, KeyAccess> getAccessedKeys() {
        if(accessedKeys == null) {
            accessedKeys = ArrayListMultimap.create();
        }
        return accessedKeys;
    }

    @Override
    public String toString() {
        return "[rid=" + rid + ", b=" + branch + ", r=" + restrain + ",redo=" + redo + "]";
    }

}
