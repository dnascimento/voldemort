package voldemort.undoTracker.branching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import voldemort.undoTracker.map.VersionShuttle;

public class BranchPath implements Serializable {

    private static final long serialVersionUID = 1L;

    public VersionShuttle current;
    public HashSet<VersionShuttle> path;

    public BranchPath(VersionShuttle current, HashSet<VersionShuttle> path) {
        super();
        this.current = current;
        this.path = path;
    }

    public BranchPath(VersionShuttle current, VersionShuttle... pathEntries) {
        this.current = current;
        this.path = new HashSet<VersionShuttle>();
        path.addAll(Arrays.asList(pathEntries));
    }

    public void getPathOrdered() {
        ArrayList<VersionShuttle> list = new ArrayList<VersionShuttle>(path);
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
