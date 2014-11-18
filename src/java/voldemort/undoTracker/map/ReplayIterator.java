package voldemort.undoTracker.map;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.utils.ByteArray;

public class ReplayIterator {

    private transient static final Logger log = Logger.getLogger(KeyMap.class.getName());

    /**
     * A reference to the full execution list
     */
    private final ArrayList<Op> fullList;

    /**
     * Allowed operations to execute now
     */
    private final ArrayList<Op> allowed = new ArrayList<Op>();

    /**
     * Allowed operations which are executing
     */
    private final ArrayList<Op> executing = new ArrayList<Op>();

    /**
     * Allowed operations which are sleeping
     */
    private final HashSet<Op> waiting = new HashSet<Op>();

    /**
     * Operations to ignore (different execution), the request was processed
     */
    private final HashSet<Long> ignoring = new HashSet<Long>();

    /**
     * Position to fetch the next allowed operations, always on the 1st element
     */
    private int nextPosition = 0;

    /**
     * Each branch has an iterator = an execution of replay
     */
    private final short branch;

    /**
     * The snapshot which the replay is based on
     */
    private long baseRid;

    // TODO replace the array list of OperationList for a linked list and
    // iterate. No need to be an array, the array is slower than the linked
    // list.
    public ReplayIterator(short branch, long baseRid, ArrayList<Op> list) {
        this.branch = branch;
        this.fullList = list;
        this.baseRid = baseRid;

        for(nextPosition = 0; nextPosition < list.size(); nextPosition++) {
            if(list.get(nextPosition).rid >= baseRid)
                break;
        }
    }

    /**
     * Allowed only if it belongs to allowed list.
     * If there are not requests executing nether allowed, update the allowed
     * list
     * 
     * @param op
     * @return true if allowed to execute, false if must go sleep
     */
    public boolean operationIsAllowed(Op op, ByteArray key) {
        // If no operation allowed or executing, fetch next
        try {
            while(allowed.isEmpty() && executing.isEmpty()) {
                fetchNextAllowed();
            }
        } catch(Exception e) {
            // the list is over
            log.warn(e.getMessage());
            return true;
        }

        if(allowed.remove(op)) {
            if(waiting.remove(op)) {
                // log.info(ByteArray.toAscii(key) + ": " + "Op " + op +
                // " was sleeping");
            }
            executing.add(op);
            return true;
        } else {
            waiting.add(op);
            log.info(ByteArray.toAscii(key) + ": " + "Op " + op + " go sleep, waiting for: "
                     + allowed);
            return false;
        }

    }

    /**
     * 
     */
    private void fetchNextAllowed() {
        assert (allowed.size() == 0);
        // all the gets or one write
        if(nextPosition == fullList.size()) {
            throw new VoldemortException("Fetch Next but list is over");
        }

        log.info("Fetch more operations");
        // is next a read?
        if(fullList.get(nextPosition).isRead()) {
            // add all the reads
            while(nextPosition < fullList.size()) {
                Op n = fullList.get(nextPosition);
                if(!n.isRead()) {
                    break;
                }
                allowed.add(fullList.get(nextPosition++));
            }
        } else {
            allowed.add(fullList.get(nextPosition++));
        }
        // remove the ignored operations
        removeIgnored();
    }

    /**
     * Removes the operations of previous snapshots or over
     */
    private void removeIgnored() {
        Iterator<Op> it = allowed.listIterator();
        while(it.hasNext()) {
            Long rid = new Long(it.next().rid);
            if(rid < baseRid || ignoring.contains(rid)) {
                it.remove();
                continue;
            }
        }
    }

    /**
     * Finish the execution of current operation
     * 
     * @param op
     * @return true if there are threads waiting to wake
     */
    public boolean endOp(Op op) {
        if(!executing.remove(op)) {
            log.error("Removing an end operation that is not at executing list");
        }
        return !waiting.isEmpty();
    }

    public short getBranch() {
        return branch;
    }

    /**
     * 
     * @param rid
     * @return true if the thread should wake other sleeping threads
     */
    public boolean ignore(long rid) {
        // remove from the allowed operations
        Iterator<Op> it = allowed.listIterator();
        while(it.hasNext()) {
            if(it.next().rid == rid)
                it.remove();
        }

        // add to ignoring list
        ignoring.add(rid);

        return !waiting.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReplayIterator [fullList=");
        sb.append(Arrays.toString(fullList.toArray()));
        sb.append("\n nextPosition=" + nextPosition + " baseRid=" + baseRid);
        sb.append("\n allowed=");
        sb.append(Arrays.toString(allowed.toArray()));
        sb.append("\n executing=");
        sb.append(Arrays.toString(executing.toArray()));
        sb.append("\n sleeping=");
        sb.append(Arrays.toString(waiting.toArray()));
        sb.append("\n ignoring=");
        sb.append(Arrays.toString(ignoring.toArray()));

        return sb.toString();
    }
}
