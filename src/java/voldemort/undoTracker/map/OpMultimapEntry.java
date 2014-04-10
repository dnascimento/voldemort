package voldemort.undoTracker.map;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import voldemort.undoTracker.map.Op.OpType;

/**
 * 
 * @author darionascimento
 * 
 */
public class OpMultimapEntry {

    /**
     * Entry RWLock: not related with list access. It is the key locker.
     */
    private RWLock lock = new RWLock();
    /**
     * Use synchronized(this) to access the list.
     */
    private LinkedList<Op> list = new LinkedList<Op>();

    public OpMultimapEntry(LinkedList<Op> list) {
        this.list = list;
    }

    public OpMultimapEntry() {}

    public void isNextGet(long rid) {
        while(true) {
            synchronized(this) {
                Iterator<Op> i = list.iterator();
                while(i.hasNext()) {
                    Op op = i.next();
                    if(!op.type.equals(OpType.Get)) {
                        break;
                    }
                    if(op.rid == rid) {
                        return;
                    }
                }
            }
            readWait();
        }
    }

    /**
     * Wait if the current item is not the expected
     * 
     * @param rid
     */
    public void isNextPut(long rid) {
        while(true) {
            synchronized(this) {
                Op op = list.getFirst();
                if(op.rid == rid)
                    return;
            }
            writeWait();
        }
    }

    public void isNextDelete(long rid) {
        while(true) {
            synchronized(this) {
                Op op = list.getFirst();
                if(op.rid == rid)
                    return;
            }
            writeWait();
        }
    }

    public void remove(long rid) {
        synchronized(this) {
            Iterator<Op> i = list.iterator();
            while(i.hasNext()) {
                if(i.next().rid == rid) {
                    i.remove();
                    break;
                }
            }
            if(!list.isEmpty()) {
                OpType nextType = list.getFirst().type;
                if(nextType.equals(OpType.Get)) {
                    notifyReads();
                } else {
                    notifyWrites();
                }
            }
        }
    }

    public void addAll(List<Op> values) {
        synchronized(this) {
            list.addAll(values);
        }
    }

    public synchronized Op getLastWrite() {
        synchronized(this) {
            Iterator<Op> i = list.descendingIterator();
            while(i.hasNext()) {
                Op op = i.next();
                if(op.type != OpType.Get) {
                    return op;
                }
            }
            return null;
        }
    }

    public boolean isEmpty() {
        synchronized(this) {
            return list.isEmpty();
        }
    }

    public void addLast(Op op) {
        synchronized(this) {
            list.addLast(op);
        }
    }

    public LinkedList<Op> getAll() {
        return list;
    }

    public LinkedList<Op> extractAll() {
        synchronized(this) {
            LinkedList<Op> data = list;
            list = new LinkedList<Op>();
            return data;
        }
    }

    // //////////// List Lock System //////////////////////

    public void lockWrite() {
        lock.lockWrite();
    }

    public void lockRead() {
        lock.lockRead();
    }

    public void unlockWrite() {
        lock.releaseWrite();
    }

    public void unlockRead() {
        lock.releaseRead();
    }

    public void readWait() {
        lock.readWait();
    }

    public void writeWait() {
        lock.writeWait();
    }

    public void notifyReads() {
        lock.notifyReads();
    }

    public void notifyWrites() {
        lock.notifyWrites();
    }

    /**
     * Count if there are some operation running
     * 
     * @return
     */
    public boolean hasLocked() {
        return lock.hasPendent();
    }

    /**
     * Get the biggest write RID (the latest version of value) which value is
     * lower than sts
     * 
     * @param sts: season timestamp: -1 if wants the latest
     * @return season timestamp of latest version which value is lower than sts
     *         or -1 if the entry is deleted or not-exists
     */
    public long getBiggestVersion(long sts) {
        // TODO optimized version could use binary search but the list would
        // need to be an array.
        synchronized(this) {
            if(sts == -1) {
                return (list.isEmpty()) ? -1 : list.peekFirst().rid;
            }
            Iterator<Op> i = list.descendingIterator();
            while(i.hasNext()) {
                Op op = i.next();
                if(op.rid >= sts)
                    continue;
                switch(op.type) {
                    case Put:
                        return op.rid;
                    case Delete:
                        return -1;
                    default:
                        break;
                }
            }
            return -1;

        }
    }
}
