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

    private final static Logger log = Logger.getLogger(BranchController.class.getName());
    public final static long INIT_COMMIT = 0L;
    public final static short INIT_BRANCH = 0;

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
    public synchronized void newCommit(long newCommit) {
        StsBranchPair newPair = new StsBranchPair(newCommit, current.current.branch);
        current.path.add(newPair);
        current.current = newPair;
    }

    /**
     * Invoked by every method to know the current
     * 
     * @return
     */
    public synchronized BranchPath getCurrent() {
        return current;
    }

    public synchronized void redoOver() {
        current = redo;
        redo = null;
        log.info("restrain phase is over, new branch is:" + current.current.branch
                 + " based on commit: " + current.current.sts);
    }

    public synchronized void newRedo(BranchPath redoPath) {
        redo = redoPath;
    }

    public synchronized Path getPath(short branch) {
        if(current.current.branch == branch) {
            return new Path(current, false);
        } else {
            return new Path(redo, true);
        }
    }

    public Boolean isRedo(short branch) {
        if(current.current.branch == branch) {
            return false;
        }
        if(redo != null && redo.current.branch == branch)
            return true;

        log.error("isRedo: branch not present: " + branch);
        throw new VoldemortException("isRedo: branch not present: " + branch);
    }

}
