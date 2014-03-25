package voldemort.undoTracker;

public class Op {

    public enum OpType {
        Write,
        Delete,
        Read;
    }

    public long rid;
    public OpType type;

    public Op(long rid, OpType type) {
        super();
        this.rid = rid;
        this.type = type;
    }
}
