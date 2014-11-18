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
        list.add(new VersionShuttle(BranchController.INIT_COMMIT, BranchController.INIT_BRANCH));
    }

    /**
     * 
     * Used by Snapshot scheduler
     * 
     * @param sts: season timestamp:
     * @return season timestamp of latest version which value is lower than sts
     */
    public VersionShuttle getBiggestSmallerSnapshot(BranchPath c, long snapshot) {
        // try to access the latest first
        VersionShuttle last = list.getLast();
        if(c.path.contains(last) && last.sid < snapshot) {
            return last;
        }

        // cache miss
        Iterator<VersionShuttle> i = list.descendingIterator();
        while(i.hasNext()) {
            VersionShuttle e = i.next();
            if(c.path.contains(e)) {
                if(e.sid < snapshot) {
                    return e;
                }
            }
        }
        log.error("getBiggestSmallerSnapshot: empty");
        throw new VoldemortException("getBiggestSmallerSnapshot: empty");

    }

    /**
     * To read the most recent version and branch
     * 
     * @param current
     * @return
     */
    public VersionShuttle getLatest(BranchPath path) {
        // try to access the latest first
        VersionShuttle last = list.getLast();
        if(path.path.contains(last)) {
            return last;
        }

        // cache miss: search
        Iterator<VersionShuttle> i = list.descendingIterator();
        while(i.hasNext()) {
            VersionShuttle e = i.next();
            if(path.path.contains(e))
                return e;
        }
        log.error("getLatest: empty");
        throw new VoldemortException("getLatest: empty");
    }

    /**
     * New snapshot. Invoked by normal requests
     * 
     * @param sts
     * @param branch
     * @return
     */
    public VersionShuttle addNewSnapshot(long sts, short branch) {
        // TODO TALVEZ TENHA DE VERIFICAR SE NAO EXISTE UM SNAPSHOT DO MESMO
        // BRANCH MAIOR
        VersionShuttle obj = new VersionShuttle(sts, branch);
        list.addLast(obj);
        return obj;
    }

    /**
     * Try to find the replayBranch and replayBaseSnapshot
     * otherwise, use one previous to the replayBaseSnapshot master.
     * Get the latest if same snapshot and same branch or the biggest smaller of
     * the previous branch
     * 
     * @param sts: snapshot used to start the new branch
     * @param branch: the replay branch
     * @return
     */
    public synchronized VersionShuttle replayRead(BranchPath path) {
        // TODO list is sorted?
        Iterator<VersionShuttle> i = list.descendingIterator();
        while(i.hasNext()) {
            VersionShuttle e = i.next();
            if(e.branch == path.current.branch && e.sid == path.current.sid) {
                // already written by the replay
                return e;
            }
            if(path.path.contains(e)) {
                if(e.sid < path.current.sid) {
                    // the latest smaller than the base replay
                    return e;
                }
            }
        }
        // it will try to access and fail, no problem
        return new VersionShuttle(path.current.sid, path.current.branch);
    }

    /**
     * The replay write is always in the new replayBranch and replayBaseSnapshot
     * 
     * @param path
     * @param replayBaseSnapshot
     * @return
     */
    public synchronized VersionShuttle replayWrite(BranchPath path) {
        VersionShuttle latest = getLatest(path);

        // if latest version is not the replay version, then add it
        if(latest.branch != path.current.branch || latest.sid != path.current.sid) {
            latest = new VersionShuttle(path.current.sid, path.current.branch);
            list.add(latest);
        }
        return latest;
    }

    public int size() {
        return list.size();
    }
}
