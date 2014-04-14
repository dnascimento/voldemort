/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.map;

public class StsBranchPair {

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

}
