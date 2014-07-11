package voldemort.undoTracker.map;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.undoTracker.map.Op.OpType;

public class RedoIterator {

    private transient static final Logger log = Logger.getLogger(OpMultimap.class.getName());

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
    private final ArrayList<Op> sleeping = new ArrayList<Op>();

    /**
     * Operations to ignore (different execution), the request was processed
     */
    private final HashSet<Long> ignoring = new HashSet<Long>();

    /**
     * Position to fetch the next allowed operations, always on the 1st element
     */
    private int nextPosition = 0;

    /**
     * Each branch has an iterator = an execution of redo
     */
    private final short branch;

    /**
     * The snapshot which the redo is based on
     */
    private long baseRid;

    public RedoIterator(short branch, long baseRid, ArrayList<Op> list) {
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
    public boolean allows(Op op) {
        // If no operation allowed or executing, fetch next
        while(allowed.isEmpty() && executing.isEmpty()) {
            fetchNextAllowed();
            removeIgnored();
        }

        if(allowed.remove(op)) {
            if(sleeping.remove(op))
                log.info("Op " + op + " was sleeping");
            executing.add(op);
            log.info("Op " + op + " allowed to exec");
            return true;
        } else {
            sleeping.add(op);
            log.info("Op " + op + " go sleep");
            return false;
        }

    }

    private void fetchNextAllowed() {
        assert (allowed.size() == 0);
        // all the gets or one write
        if(nextPosition == fullList.size()) {
            throw new VoldemortException("Fetch Next but list is over");
        }

        log.info("Fetch more operations");
        // is next a write?
        if(fullList.get(nextPosition).type != OpType.Get) {
            allowed.add(fullList.get(nextPosition++));
            return;
        }
        // add all the reads
        while(nextPosition < fullList.size()) {
            if(fullList.get(nextPosition).type != OpType.Get) {
                break;
            }
            allowed.add(fullList.get(nextPosition++));
        }
    }

    private void removeIgnored() {
        // remove the operations of previous snapshots or over
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
        return !sleeping.isEmpty();
    }

    public short getBranch() {
        return branch;
    }

    public boolean ignore(long rid) {
        // executed, jump the executing list
        Iterator<Op> it = allowed.listIterator();
        while(it.hasNext()) {
            if(it.next().rid == rid)
                it.remove();
        }
        ignoring.add(rid);

        return !sleeping.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RedoIterator [fullList=");
        sb.append(Arrays.toString(fullList.toArray()));
        sb.append("\n nextPosition=" + nextPosition + " baseRid=" + baseRid);
        sb.append("\n allowed=");
        sb.append(Arrays.toString(allowed.toArray()));
        sb.append("\n executing=");
        sb.append(Arrays.toString(executing.toArray()));
        sb.append("\n sleeping=");
        sb.append(Arrays.toString(sleeping.toArray()));
        sb.append("\n ignoring=");
        sb.append(Arrays.toString(ignoring.toArray()));

        return sb.toString();
    }
}
