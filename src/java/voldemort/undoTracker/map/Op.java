/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */
package voldemort.undoTracker.map;

import java.io.Serializable;

public class Op implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public enum OpType implements Serializable {
        Put,
        Delete,
        Get;
    }

    public long rid;
    public long sts;
    public OpType type;

    public Op(long rid, OpType type) {
        super();
        this.rid = rid;
        this.type = type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (rid ^ (rid >>> 32));
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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
        Op other = (Op) obj;
        if(rid != other.rid)
            return false;
        if(type != other.type)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "[" + type + " " + "rid=" + rid + ", sts=" + sts + "]";
    }
}
