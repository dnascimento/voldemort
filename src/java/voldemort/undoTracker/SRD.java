/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker;

import pt.inesc.undo.proto.ToManagerProto;
import voldemort.utils.ByteArray;

import com.google.common.collect.ArrayListMultimap;

/**
 * Shuttle Request Data
 * 
 * @author darionascimento
 * 
 */
public class SRD {

    public enum OpType {
        Put,
        Delete,
        Get,
        GetVersion;
    }

    public final long rid;
    public short branch;
    public final boolean restrain;
    public ArrayListMultimap<ByteArray, KeyAccess> accessedKeys;
    public final boolean replay;

    public SRD() {
        this(0L, 0, false, false);
    }

    public SRD(long rid) {
        this(rid, 0, false, false);
    }

    public SRD(ToManagerProto.MsgToManager.SRD srd) {
        this(srd.getRid(), srd.getBranch(), srd.getRestrain());
    }

    public SRD(long rid, int branch, boolean restrain) {
        this(rid, branch, restrain, false);
    }

    public SRD(long rid, int branch, boolean restrain, boolean replay) {
        this.rid = rid;
        this.branch = (short) branch;
        this.restrain = restrain;
        this.replay = replay;
        this.accessedKeys = null;
    }

    public ToManagerProto.MsgToManager.SRD toProto() {
        return ToManagerProto.MsgToManager.SRD.newBuilder()
                                              .setBranch(branch)
                                              .setRid(rid)
                                              .setRestrain(restrain)
                                              .build();
    }

    public void addAccessedKey(ByteArray key, String storeName, OpType type) {
        if(accessedKeys == null)
            accessedKeys = ArrayListMultimap.create();
        KeyAccess k = new KeyAccess(storeName, type);
        int index = accessedKeys.get(key).indexOf(k);
        if(index == -1) {
            accessedKeys.put(key, k);
        } else {
            accessedKeys.get(key).get(index).times += 1;
        }
    }

    public ArrayListMultimap<ByteArray, KeyAccess> getAccessedKeys() {
        if(accessedKeys == null) {
            accessedKeys = ArrayListMultimap.create();
        }
        return accessedKeys;
    }

    @Override
    public String toString() {
        return "[rid=" + rid + ", b=" + branch + ", r=" + restrain + ",replay=" + replay + "]";
    }

}
