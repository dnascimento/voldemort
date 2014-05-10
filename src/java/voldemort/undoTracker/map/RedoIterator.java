package voldemort.undoTracker.map;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import voldemort.VoldemortException;
import voldemort.undoTracker.map.Op.OpType;

public class RedoIterator implements Iterator<Op> {

    private final HashSet<Long> unlockedSet = new HashSet<Long>();
    private final ArrayList<Op> list;
    private final short branch;

    private int position;
    private Op next;
    private Op read;

    public RedoIterator(short branch, long baseRid, ArrayList<Op> list) {
        this.branch = branch;
        this.list = list;

        // TODO analyze the cost, may binary search would help
        for(position = 0; position < list.size(); position++) {
            if(list.get(position).rid >= baseRid)
                break;
        }
        position--;
    }

    @Override
    public boolean hasNext() {
        if(++position == list.size())
            return false;

        // ignore the unlocked operations
        next = list.get(position);
        while(unlockedSet.remove(next.rid)) {
            if(++position == list.size())
                return false;
            next = list.get(position);
        }
        return true;
    }

    @Override
    public Op next() {
        if(next == null)
            hasNext();
        return next;
    }

    @Override
    public void remove() {
        // LIST IS NOT MODIFICABLE - EMPTY TO PROTECT
        throw new VoldemortException("Not removable during redo");
    }

    public boolean unlock(long rid) {
        unlockedSet.add(rid);
        return true;
    }

    public boolean hasRead(int i) {
        if(position + i >= list.size()) {
            return false;
        }
        if(list.get(position + i).type.equals(OpType.Get)) {
            read = list.get(position + i);
            return true;
        } else {
            return false;
        }
    }

    public Op peakRead() {
        return read;
    }

    public short getBranch() {
        return branch;
    }
}
