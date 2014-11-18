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

    public long sid;
    public short branch;

    public VersionShuttle(long sts, int branch) {
        this(sts, (short) branch);
    }

    public VersionShuttle(long sid, short branch) {
        super();
        this.sid = sid;
        this.branch = branch;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + branch;
        result = prime * result + (int) (sid ^ (sid >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "[sts=" + sid + ", branch=" + branch + "]";
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
        if(branch != other.branch)
            return false;
        if(sid != other.sid)
            return false;
        return true;
    }

    @Override
    public int compareTo(VersionShuttle o) {
        if(o.branch != branch) {
            return branch - o.branch;
        }
        return (int) (sid - o.sid);
    }

}
