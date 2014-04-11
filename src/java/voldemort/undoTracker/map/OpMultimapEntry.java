package voldemort.undoTracker.map;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.undoTracker.map.Op.OpType;

import com.google.common.collect.HashMultimap;

/**
 * 
 * @author darionascimento
 * 
 */
public class OpMultimapEntry {

    private final Logger log = LogManager.getLogger("OpMultimapEntry");

    /**
     * Avoid re-size version array, average number of version per key
     */
    public final int INIT_ARRAY_SIZE = 16;
    /**
     * Entry RWLock: not related with list access. It is the key locker.
     */
    private RWLock lock = new RWLock();
    /**
     * Use synchronized(this) to access the list.
     */
    private ArrayList<Op> list = new ArrayList<Op>(INIT_ARRAY_SIZE);

    /**
     * Keep track of next or last write
     */
    private int redoPos = 0;
    /**
     * Keep track of last sent dependency
     */
    private int sentDependency = 0;

    public OpMultimapEntry(ArrayList<Op> list) {
        this.list = list;
    }

    public OpMultimapEntry() {

    }

    /**
     * 
     * @param rid
     */
    public void isNextGet(long rid) {
        while(true) {
            synchronized(this) {
                int p = redoPos;
                while(p < list.size()) {
                    Op op = list.get(p);
                    if(!op.type.equals(OpType.Get)) {
                        break;
                    }
                    if(op.rid == rid) {
                        return;
                    }
                    p++;
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
                Op op = list.get(redoPos);
                if(op.rid == rid) {
                    return;
                }
            }
            writeWait();
        }
    }

    public void isNextDelete(long rid) {
        while(true) {
            synchronized(this) {
                Op op = list.get(redoPos);
                if(op.rid == rid) {
                    return;
                }
            }
            writeWait();
        }
    }

    public void endOp(OpType type) {
        synchronized(this) {
            if(list.size() == redoPos + 1) {
                return;
            }

            Op next = list.get(++redoPos);
            if(type.equals(OpType.Get)) {
                if(!next.type.equals(OpType.Get)) {
                    // read and next is a write
                    notifyWrites();
                }
            } else {
                // write, then unlock the next
                if(next.type.equals(OpType.Get)) {
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
            for(int i = list.size() - 1; i > 0; i--) {
                Op op = list.get(i);
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
            list.add(op);
        }
    }

    public ArrayList<Op> getAll() {
        return list;
    }

    public ArrayList<Op> extractAll() {
        synchronized(this) {
            ArrayList<Op> data = list;
            list = new ArrayList<Op>();
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
     */
    private long getBiggestVersion(long sts) {
        for(int i = list.size() - 1; i >= 0; i--) {
            Op op = list.get(i);
            if(op.rid > sts)
                continue;
            switch(op.type) {
                case Put:
                    return op.sts;
                case Delete:
                    return sts;
                default:
                    break;
            }

        }
        // if empty entry, write sts=0
        return 0;
    }

    public void updateDependencies(HashMultimap<Long, Long> dependencyPerRid) {
        // 1st get previous write
        synchronized(this) {
            synchronized(dependencyPerRid) {
                assert (list.size() > 0);
                long lastWrite = -1;
                for(int i = sentDependency; i >= 0; i--) {
                    if(list.get(i).type != OpType.Get) {
                        lastWrite = list.get(i).rid;
                        break;
                    }
                }
                assert (lastWrite != -1);
                log.debug("LAST WRITE:" + lastWrite);

                for(int i = sentDependency + 1; i < list.size(); i++) {
                    Op op = list.get(i);
                    if(op.type == Op.OpType.Get) {
                        dependencyPerRid.put(op.rid, lastWrite);
                    } else {
                        lastWrite = op.rid;
                    }
                }
                sentDependency = list.size() - 1;
            }
        }
    }

    /**
     * Choose the correct snapshot to access in reads and which will write
     * 
     * @param type
     * @param rid
     * @param sts
     * @return
     */
    public long trackAccessNewRequest(OpType type, long rid, long sts) {
        long snapshotAccess;
        // Store the sts of write (version of entry)
        Op op = new Op(rid, type);

        if(rid < sts) {
            // read only old values
            snapshotAccess = getBiggestVersion(sts);
            if(!type.equals(Op.OpType.Get)) {
                op.sts = snapshotAccess;
            }
        } else {
            // read the most recent
            if(type.equals(Op.OpType.Get)) {
                snapshotAccess = getBiggestVersion(Long.MAX_VALUE);
            } else {
                snapshotAccess = sts;
                op.sts = sts;
            }
        }
        assert (snapshotAccess != -1);

        addLast(op);
        return snapshotAccess;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        synchronized(this) {
            if(this == obj)
                return true;
            if(obj == null)
                return false;
            if(getClass() != obj.getClass())
                return false;
            OpMultimapEntry other = (OpMultimapEntry) obj;
            if(list == null) {
                if(other.list != null)
                    return false;
            } else if(!list.equals(other.list))
                return false;
            return true;
        }
    }

    @Override
    public String toString() {
        synchronized(this) {

            return "[Entry:" + list + ", redoPos=" + redoPos + ", sentDependency=" + sentDependency
                   + "]";
        }
    }

    public int size() {
        synchronized(this) {
            return list.size();
        }
    }

}
