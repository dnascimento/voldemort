package voldemort.undoTracker.branching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import voldemort.undoTracker.map.StsBranchPair;

public class BranchPath {

    public StsBranchPair current;
    public HashSet<StsBranchPair> path;

    public BranchPath(StsBranchPair current, HashSet<StsBranchPair> path) {
        super();
        this.current = current;
        this.path = path;
    }

    public BranchPath(StsBranchPair current, StsBranchPair... pathEntries) {
        this.current = current;
        this.path = new HashSet<StsBranchPair>();
        path.addAll(Arrays.asList(pathEntries));
    }

    public void getPathOrdered() {
        ArrayList<StsBranchPair> list = new ArrayList<StsBranchPair>(path);
        Collections.sort(list);
    }

    @Override
    public String toString() {
        return "BranchPath [current=" + current + ", path=" + path + "]";
    }

}
