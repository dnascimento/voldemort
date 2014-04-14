/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker;

import undo.proto.ToManagerProto;

/**
 * Request Undo Data
 * 
 * @author darionascimento
 * 
 */
public class RUD {

    public long rid;
    public short branch;
    public boolean restrain;

    public RUD() {
        this(0L, 0, false);
    }

    public RUD(long rid) {
        this(rid, 0, false);
    }

    public RUD(undo.proto.ToManagerProto.RUD rud) {
        this(rud.getRid(), rud.getBranch(), rud.getRestrain());
    }

    public RUD(long rid, int branch, boolean restrain) {
        this.rid = rid;
        this.branch = (short) branch;
        this.restrain = restrain;
    }

    public ToManagerProto.RUD toProto() {
        return ToManagerProto.RUD.newBuilder()
                                 .setBranch(branch)
                                 .setRid(rid)
                                 .setRestrain(restrain)
                                 .build();
    }

    @Override
    public String toString() {
        return "[rid=" + rid + ", b=" + branch + ", r=" + restrain + "]";
    }

}
