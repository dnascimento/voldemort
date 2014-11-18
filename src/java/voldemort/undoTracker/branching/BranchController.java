/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.branching;

import java.io.Serializable;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.undoTracker.map.VersionShuttle;

public class BranchController implements Serializable {

    private static final long serialVersionUID = 1L;
    transient private static final Logger log = Logger.getLogger(BranchController.class.getName());
    public static final long INIT_COMMIT = 0L;
    public static final short INIT_BRANCH = 0;

    BranchPath current;
    BranchPath replay;

    public BranchController() {
        VersionShuttle baseBranch = new VersionShuttle(INIT_COMMIT, INIT_BRANCH);
        current = new BranchPath(baseBranch, baseBranch);
        replay = null;
    }

    /**
     * Invoked by manger to start a new snapshot in future, in the current branch
     * 
     * @param newRid
     */
    public void newSnapshot(long newSnapshot) {
        VersionShuttle newPair = new VersionShuttle(newSnapshot, current.current.branch);
        // TODO may cause problem if some thread is checking the path
        current.path.add(newPair);
        current.current = newPair;
    }

    /**
     * Invoked by restrain request to get the most recent
     * 
     * @return
     */
    public BranchPath getCurrent() {
        return current;
    }

    /**
     * Make the replay branch become the current branch
     * 
     * @return the new current branch
     */
    public short replayOver() {
        current = replay;
        // TODO may cause problem if some thread is checking the path
        replay = null;
        log.info("restrain phase is over, new branch is:" + current.current.branch
                 + " based on snapshot: " + current.current.sid);
        return current.current.branch;
    }

    /**
     * Invoked by the manager before starting the recovery process
     * 
     * @param replayPath
     */
    public synchronized void newReplay(BranchPath replayPath) {
        log.info("New replay: " + replayPath);
        replay = replayPath;
    }

    /**
     * Get the path for this branch
     * 
     * @param the branch used by the request
     * @return the path of this branch and if is replay or not
     */
    public Path getPath(short branch) {
        if(current.current.branch == branch) {
            return new Path(current, false);
        }

        if(replay != null && replay.current.branch == branch) {
            return new Path(replay, true);
        }

        log.error("getPath: branch not present: " + branch);
        throw new VoldemortException("isReplay: branch not present: " + branch);
    }

    public void reset() {
        VersionShuttle baseBranch = new VersionShuttle(INIT_COMMIT, INIT_BRANCH);
        current = new BranchPath(baseBranch, baseBranch);
        log.info("RESET");
        replay = null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((current == null) ? 0 : current.hashCode());
        result = prime * result + ((replay == null) ? 0 : replay.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        BranchController other = (BranchController) obj;
        if(current == null) {
            if(other.current != null)
                return false;
        } else if(!current.equals(other.current))
            return false;
        if(replay == null) {
            if(other.replay != null)
                return false;
        } else if(!replay.equals(other.replay))
            return false;
        return true;
    }

}
