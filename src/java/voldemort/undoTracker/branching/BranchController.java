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

public class BranchController implements Serializable {

    private static final long serialVersionUID = 1L;
    transient private static final Logger log = Logger.getLogger(BranchController.class.getName());
    public static final long ROOT_SNAPSHOT = 0L;
    public static final short ROOT_BRANCH = 0;

    BranchPath currentPath;
    BranchPath replayPath;

    public BranchController() {
        currentPath = new BranchPath(ROOT_BRANCH, ROOT_SNAPSHOT);
        replayPath = null;
    }

    /**
     * Invoked by manger to start a new snapshot in future, in the current
     * branch
     * 
     * @param newRid
     */
    public void newSnapshot(long newSnapshot) {
        currentPath.newSnapshot(newSnapshot);
    }

    /**
     * Invoked by restrain request to get the most recent
     * 
     * @return
     */
    public BranchPath getCurrent() {
        return currentPath;
    }

    /**
     * Make the replay branch become the current branch
     * 
     * @return the new current branch
     */
    public short replayOver() {
        currentPath = replayPath;
        // TODO may cause problem if some thread is checking the path
        replayPath = null;
        log.info("restrain phase is over, new branch is:" + currentPath.branch
                 + " based on snapshot: " + currentPath.latestVersion.sid);
        return currentPath.branch;
    }

    /**
     * Invoked by the manager before starting the recovery process
     * 
     * @param replayPath
     */
    public synchronized void newReplay(BranchPath replayPath) {
        log.info("New replay: " + replayPath);
        this.replayPath = replayPath;
    }

    /**
     * Get the path for this branch
     * 
     * @param the branch used by the request
     * @return the path of this branch and if is replay or not
     */
    public Path getPath(short branch) {
        if(currentPath.branch == branch) {
            return new Path(currentPath, false);
        }
        // if the replayPath exists and have same branch
        if(replayPath != null && replayPath.branch == branch) {
            return new Path(replayPath, true);
        }

        log.error("getPath: branch not present: " + branch);
        throw new VoldemortException("isReplay: branch not present: " + branch);
    }

    public void reset() {
        currentPath = new BranchPath(ROOT_BRANCH, ROOT_SNAPSHOT);
        log.info("RESET");
        replayPath = null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((currentPath == null) ? 0 : currentPath.hashCode());
        result = prime * result + ((replayPath == null) ? 0 : replayPath.hashCode());
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
        if(currentPath == null) {
            if(other.currentPath != null)
                return false;
        } else if(!currentPath.equals(other.currentPath))
            return false;
        if(replayPath == null) {
            if(other.replayPath != null)
                return false;
        } else if(!replayPath.equals(other.replayPath))
            return false;
        return true;
    }

}
