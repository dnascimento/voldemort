/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.map;

import java.io.Serializable;

public class VersionShuttle implements Serializable, Comparable<VersionShuttle> {

    private transient static final long serialVersionUID = 1L;
    /**
     * A version is created when a snapshot is written by the firt time
     */
    public long sid;

    public VersionShuttle(long sid) {
        this.sid = sid;
    }

    @Override
    public int compareTo(VersionShuttle o) {
        return (int) (sid - o.sid);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (sid ^ (sid >>> 32));
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
        VersionShuttle other = (VersionShuttle) obj;
        if(sid != other.sid)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "[" + sid + "]";
    }

}
