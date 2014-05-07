package voldemort.undoTracker;

import voldemort.utils.ByteArray;

public class KeyAccess {

    public ByteArray key;
    public String store;

    public KeyAccess(ByteArray key, String store) {
        super();
        this.key = key;
        this.store = store;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((store == null) ? 0 : store.hashCode());
        return result;
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
        if(key == null) {
            if(other.key != null)
                return false;
        } else if(!key.equals(other.key))
            return false;
        if(store == null) {
            if(other.store != null)
                return false;
        } else if(!store.equals(other.store))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return ByteArray.toAscii(key) + "|" + store;
    }

}
