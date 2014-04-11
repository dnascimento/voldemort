package voldemort.undoTracker.map;

public class StsBranchPair {

    public long sts;
    public short branch;

    public StsBranchPair(long sts, int branch) {
        this(sts, (short) branch);
    }

    public StsBranchPair(long sts, short branch) {
        super();
        this.sts = sts;
        this.branch = branch;
    }

}
