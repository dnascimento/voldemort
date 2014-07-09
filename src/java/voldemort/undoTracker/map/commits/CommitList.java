package voldemort.undoTracker.map.commits;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.undoTracker.branching.BranchController;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.map.StsBranchPair;

public class CommitList implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient final static Logger log = Logger.getLogger(CommitList.class);

    /**
     * Commit-Branch list sorted by commit
     */
    LinkedList<StsBranchPair> list;

    public CommitList() {
        list = new LinkedList<StsBranchPair>();
        list.add(new StsBranchPair(BranchController.INIT_COMMIT, BranchController.INIT_BRANCH));
    }

    public CommitEntry add(long sts, short branch, boolean b) {
        CommitEntry e = new CommitEntry(sts, branch, b);
        list.add(e);
        return e;
    }

    /**
     * 
     * Used by Commit scheduler
     * 
     * @param sts: season timestamp:
     * @return season timestamp of latest version which value is lower than sts
     */
    public StsBranchPair getBiggestSmallerCommit(BranchPath c, long commit) {
        Iterator<StsBranchPair> i = list.descendingIterator();
        while(i.hasNext()) {
            StsBranchPair e = i.next();
            if(c.path.contains(e)) {
                if(e.sts < commit) {
                    return e;
                }
            }
        }
        log.error("getBiggestSmallerCommit: empty");
        throw new VoldemortException("getBiggestSmallerCommit: empty");

    }

    /**
     * To read the most recent version and branch
     * 
     * @param current
     * @return
     */
    public StsBranchPair getLatest(BranchPath path) {
        Iterator<StsBranchPair> i = list.descendingIterator();
        while(i.hasNext()) {
            StsBranchPair e = i.next();
            if(path.path.contains(e))
                return e;
        }
        log.error("getLatest: empty");
        throw new VoldemortException("getLatest: empty");
    }

    /**
     * New commit. Invoked by normal requests
     * 
     * @param sts
     * @param branch
     * @return
     */
    public StsBranchPair addNewCommit(long sts, short branch) {
        // TODO TALVEZ TENHA DE VERIFICAR SE NAO EXISTE UM SNAPSHOT DO MESMO
        // BRANCH MAIOR
        StsBranchPair obj = new StsBranchPair(sts, branch);
        list.addLast(obj);
        return obj;
    }

    /**
     * Try to find the redoBranch and redoBaseCommit (if exists, its the latest)
     * otherwise, use one previous to the redoBaseCommit master
     * 
     * @param sts: commit used to start the new branch
     * @param branch: the redo branch
     * @return
     */
    public synchronized StsBranchPair redoRead(BranchPath path) {
        return getLatest(path);
    }

    /**
     * The redo write is always in the new redoBranch and redoBaseCommit
     * 
     * @param path
     * @param redoBaseCommit
     * @return
     */
    public synchronized StsBranchPair redoWrite(BranchPath path) {
        StsBranchPair latest = getLatest(path);

        if(latest.branch == path.current.branch && latest.sts == path.current.sts) {
            return latest;
        } else {
            latest = new StsBranchPair(path.current.sts, path.current.branch);
            list.add(latest);
            return latest;
        }
    }
}
