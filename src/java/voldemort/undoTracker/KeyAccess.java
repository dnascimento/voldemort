package voldemort.undoTracker;

import voldemort.undoTracker.RUD.OpType;
import voldemort.utils.ByteArray;

public class KeyAccess implements Comparable<KeyAccess> {

    public String store;
    public int times;
    public OpType type;
    public ByteArray key;

    public KeyAccess(String store, OpType type) {
        this(store, type, 1);
    }

    public KeyAccess(String store, OpType type, int times) {
        super();
        this.store = store;
        this.type = type;
        this.times = times;
    }

    public void setKey(ByteArray key) {
        this.key = key;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((store == null) ? 0 : store.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return store + ":" + times;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        KeyAccess other = (KeyAccess) obj;
        if(store == null) {
            if(other.store != null)
                return false;
        } else if(!store.equals(other.store))
            return false;
        return true;
    }

    @Override
    public int compareTo(KeyAccess o) {
        return store.compareTo(o.store);
    }

}
