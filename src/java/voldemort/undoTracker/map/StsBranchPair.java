/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.map;

import java.io.Serializable;

public class StsBranchPair implements Serializable, Comparable<StsBranchPair> {

    private static final long serialVersionUID = 1L;

    public long sts;
    public short branch;

    public StsBranchPair(long sts, int branch) {
        this(sts, (short) branch);
    }

    public StsBranchPair(long sts, short branch) {
        super();
        this.sts = sts;
        this.branch = branch;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + branch;
        result = prime * result + (int) (sts ^ (sts >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "[sts=" + sts + ", branch=" + branch + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        StsBranchPair other = (StsBranchPair) obj;
        if(branch != other.branch)
            return false;
        if(sts != other.sts)
            return false;
        return true;
    }

    @Override
    public int compareTo(StsBranchPair o) {
        if(o.branch != branch) {
            return branch - o.branch;
        }
        return (int) (sts - o.sts);
    }

}
