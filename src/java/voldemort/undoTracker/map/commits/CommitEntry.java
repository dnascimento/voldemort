package voldemort.undoTracker.map.commits;

import voldemort.undoTracker.map.StsBranchPair;

public class CommitEntry extends StsBranchPair {

    public boolean master;

    public CommitEntry(long sts, int branch, boolean master) {
        super(sts, branch);
        this.master = master;
    }
}
