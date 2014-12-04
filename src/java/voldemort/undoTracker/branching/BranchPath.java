package voldemort.undoTracker.branching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import voldemort.undoTracker.map.VersionShuttle;

public class BranchPath implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The branch which this path refers to
     */
    public short branch;
    /**
     * The latest version in this branch
     */
    public VersionShuttle latestVersion;

    public VersionShuttle forkedVersion;

    /**
     * The all versions in this branch
     */
    public HashSet<VersionShuttle> versions;

    /**
     * Create a branch (from protobuff in ServiceDBProxy)
     * 
     * @param branch
     * @param l
     * @param list
     */
    public BranchPath(short branch, long latestVersion, List<Long> versions) {
        this.branch = branch;
        this.latestVersion = new VersionShuttle(latestVersion);
        HashSet<VersionShuttle> path = new HashSet<VersionShuttle>();
        for(Long l: versions) {
            path.add(new VersionShuttle(l));
        }

        // ensure that current version is in versions
        if(path.contains(new VersionShuttle(latestVersion))) {
            path.add(new VersionShuttle(latestVersion));
        }
        forkedVersion = getForked(path, latestVersion);
        this.versions = path;
    }

    /**
     * Initial branch
     * 
     * @param rootBranch
     * @param rootSnapshot
     */
    public BranchPath(short rootBranch, long rootSnapshot) {
        this(rootBranch, rootSnapshot, Arrays.asList(rootSnapshot));
    }

    public void newSnapshot(long newSnapshot) {
        VersionShuttle newVersion = new VersionShuttle(newSnapshot);
        // TODO may cause problem if some thread is checking the path
        versions.add(newVersion);
        latestVersion = newVersion;
    }

    public void getPathOrdered() {
        ArrayList<VersionShuttle> list = new ArrayList<VersionShuttle>(versions);
        Collections.sort(list);
    }

    @Override
    public String toString() {
        return "BranchPath [branch=" + branch + " currentVersion=" + latestVersion + ", path="
               + versions + "]";
    }

    /**
     * Get the version forked is the biggest version, except the current
     * 
     * @param path
     * @param current
     * @return
     */
    private VersionShuttle getForked(HashSet<VersionShuttle> path, long current) {
        if(path.size() == 1) {
            return null;
        }
        long biggest = 0;
        Iterator<VersionShuttle> it = path.iterator();
        while(it.hasNext()) {
            long v = it.next().sid;
            if(v < current && v > biggest) {
                biggest = v;
            }
        }
        return new VersionShuttle(biggest);
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
        if(latestVersion == null) {
            if(other.latestVersion != null)
                return false;
        } else if(!latestVersion.equals(other.latestVersion))
            return false;
        if(versions == null) {
            if(other.versions != null)
                return false;
        } else if(!versions.equals(other.versions))
            return false;
        return true;
    }
}
