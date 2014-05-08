/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.RUD;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.utils.ByteArray;

public interface AccessSchedule {

    public StsBranchPair getStart(ByteArray key, RUD rud, StsBranchPair current);

    public StsBranchPair putStart(ByteArray key, RUD rud, StsBranchPair sts);

    public StsBranchPair deleteStart(ByteArray key, RUD rud, StsBranchPair sts);

    public StsBranchPair getVersionStart(ByteArray clone, RUD rud, StsBranchPair sts);

    public void getEnd(ByteArray key, RUD rud);

    public void putEnd(ByteArray key, RUD rud);

    public void deleteEnd(ByteArray key, RUD rud);

}
