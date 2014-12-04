package voldemort.undoTracker;

import java.io.Serializable;

import org.apache.log4j.Logger;

import voldemort.undoTracker.branching.BranchController;
import voldemort.undoTracker.map.KeyMap;

/**
 * Locker used to restrain/block new requests during the final phase of the
 * replay process.
 * It blocks the new requests until the replay is over. In order to prevent a
 * request from locking after the replay process is over, this object keeps the
 * current branch (the one used by new requests, not by the requests being
 * replayed).
 * 
 * @author darionascimento
 * 
 */
public class RestrainLocker implements Serializable {

    private static final long serialVersionUID = 1L;
    private transient static final Logger log = Logger.getLogger(KeyMap.class.getName());
    /**
     * Sets the branch that will restrain (the current branch.
     */
    short branch = BranchController.ROOT_BRANCH;

    /**
     * Set the new branch to lock
     * 
     * @param newCurrentBranch
     */
    public synchronized void replayOver(short currentBranch) {
        this.branch = currentBranch;
    }

    public synchronized void restrainRequest(short branch) {
        if(this.branch == branch) {
            try {
                this.wait();
            } catch(InterruptedException e) {
                log.error("Restrain Wait in flag", e);
            }
        }
    }
}
