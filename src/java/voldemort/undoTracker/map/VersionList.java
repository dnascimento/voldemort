package voldemort.undoTracker.map;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.undoTracker.branching.BranchController;
import voldemort.undoTracker.branching.BranchPath;

public class VersionList implements Serializable {

    private transient static final long serialVersionUID = 1L;

    private transient static final Logger log = Logger.getLogger(VersionList.class);

    /**
     * Snapshot-Branch list sorted by snapshot
     */
    LinkedList<VersionShuttle> list;

    public VersionList() {
        list = new LinkedList<VersionShuttle>();
        list.add(new VersionShuttle(BranchController.ROOT_SNAPSHOT));
    }

    /**
     * 
     * Used by Snapshot scheduler
     * 
     * @param sts: season timestamp:
     * @return season timestamp of latest version which value is lower than sts
     */
    public VersionShuttle getLastestVersionPreviousToSnapshot(BranchPath path, long snapshot) {
        // try to access the latest first
        VersionShuttle latestVersion = list.getLast();
        if(path.versions.contains(latestVersion) && latestVersion.sid < snapshot) {
            return latestVersion;
        }

        // cache miss
        Iterator<VersionShuttle> i = list.descendingIterator();
        while(i.hasNext()) {
            VersionShuttle v = i.next();
            if(path.versions.contains(v)) {
                if(v.sid < snapshot) {
                    return v;
                }
            }
        }
        log.error("getBiggestSmallerSnapshot: empty");
        throw new VoldemortException("getBiggestSmallerSnapshot: empty");

    }

    /**
     * To read the most recent version and branch
     * 
     * @param currentPath
     * @return
     */
    public VersionShuttle getLatestVersionInPath(BranchPath path) {
        // try to access the latest first
        VersionShuttle last = list.getLast();
        if(path.versions.contains(last)) {
            return last;
        }

        // cache miss: search
        Iterator<VersionShuttle> i = list.descendingIterator();
        VersionShuttle e;
        while(i.hasNext()) {
            e = i.next();
            if(path.versions.contains(e))
                return e;
        }
        log.error("getLatest: empty");
        throw new VoldemortException("getLatest: empty");
    }

    /**
     * New snapshot. Invoked by normal requests
     * 
     * @param sts
     * @return
     */
    public VersionShuttle addNewVersion(long version) {
        VersionShuttle obj = new VersionShuttle(version);
        list.addLast(obj);
        return obj;
    }

    /**
     * Try to find the latest version in the path to read
     * 
     * @param sts: snapshot used to start the new branch
     * @return the version to read
     */
    public synchronized VersionShuttle replayRead(BranchPath path) {
        try {
            VersionShuttle latest = getLatestVersionInPath(path);
            return latest;
        } catch(VoldemortException e) {
            log.info(e);
            // it will try to access and fail
            return new VersionShuttle(path.latestVersion.sid);
        }
    }

    /**
     * The replay write is always in the new replayBranch and replayBaseSnapshot
     * 
     * @param path
     * @param replayBaseSnapshot
     * @return
     */
    public synchronized VersionShuttle replayWrite(BranchPath path) {
        VersionShuttle latest = getLatestVersionInPath(path);
        // if the item does not have the latest version, create it
        if(latest.sid != path.latestVersion.sid) {
            return addNewVersion(path.latestVersion.sid);
        }
        return latest;
    }

    public int size() {
        return list.size();
    }
}
