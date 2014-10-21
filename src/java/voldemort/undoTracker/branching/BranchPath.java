package voldemort.undoTracker.branching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import voldemort.undoTracker.map.StsBranchPair;

public class BranchPath implements Serializable {

    private static final long serialVersionUID = 1L;

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

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        BranchPath other = (BranchPath) obj;
        if(current == null) {
            if(other.current != null)
                return false;
        } else if(!current.equals(other.current))
            return false;
        if(path == null) {
            if(other.path != null)
                return false;
        } else if(!path.equals(other.path))
            return false;
        return true;
    }

}
