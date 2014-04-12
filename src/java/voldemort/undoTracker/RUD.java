package voldemort.undoTracker;

import voldemort.undoTracker.proto.ToManagerProto;

/**
 * Request Undo Data
 * 
 * @author darionascimento
 * 
 */
public class RUD {

    // public RUD(voldemort.undoTracker.proto.ToManagerProto.RUD rud2) {
    // // TODO
    // }

    public RUD() {
        this.rid = 0L;
    }

    public RUD(long rid, short branch, boolean restrain) {
        super();
        this.rid = rid;
        this.branch = branch;
        this.restrain = restrain;
    }

    public RUD(long rid) {
        this.rid = rid;
    }

    public RUD(voldemort.undoTracker.proto.ToManagerProto.RUD rud) {
        // TODO
    }

    public long rid;
    public short branch;
    public boolean restrain;

    public ToManagerProto.RUD toProto() {
        return ToManagerProto.RUD.newBuilder()
                                 .setBranch(branch)
                                 .setRid(rid)
                                 .setRestrain(restrain)
                                 .build();
    }
}
