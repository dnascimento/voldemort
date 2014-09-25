/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.branching;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.undoTracker.map.StsBranchPair;

public class BranchController {

    private static final Logger log = Logger.getLogger(BranchController.class.getName());
    public static final long INIT_COMMIT = 0L;
    public static final short INIT_BRANCH = 0;

    BranchPath current;
    BranchPath redo;

    public BranchController() {
        StsBranchPair baseBranch = new StsBranchPair(INIT_COMMIT, INIT_BRANCH);
        current = new BranchPath(baseBranch, baseBranch);
        redo = null;
    }

    /**
     * Invoked by manger to start a new commit in future, in the current branch
     * 
     * @param newRid
     */
    public void newCommit(long newCommit) {
        StsBranchPair newPair = new StsBranchPair(newCommit, current.current.branch);
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

    public void redoOver() {
        current = redo;
        // TODO may cause problem if some thread is checking the path
        redo = null;
        log.info("restrain phase is over, new branch is:" + current.current.branch
                 + " based on commit: " + current.current.sts);
    }

    /**
     * Invoked by the manager before starting the recovery process
     * 
     * @param redoPath
     */
    public synchronized void newRedo(BranchPath redoPath) {
        redo = redoPath;
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

        if(redo != null && redo.current.branch == branch) {
            return new Path(redo, true);
        }

        log.error("getPath: branch not present: " + branch);
        throw new VoldemortException("isRedo: branch not present: " + branch);
    }

    public void reset() {
        StsBranchPair baseBranch = new StsBranchPair(INIT_COMMIT, INIT_BRANCH);
        current = new BranchPath(baseBranch, baseBranch);
        redo = null;
    }

}
