/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker.schedulers;

import voldemort.undoTracker.RUD;
import voldemort.utils.ByteArray;

public interface AccessSchedule {

    public void getEnd(ByteArray key, RUD rud);

    public void putEnd(ByteArray key, RUD rud);

    public void deleteEnd(ByteArray key, RUD rud);

    public long getStart(ByteArray key, RUD rud, long sts);

    public long putStart(ByteArray key, RUD rud, long sts);

    public long deleteStart(ByteArray key, RUD rud, long sts);

    long getVersionStart(ByteArray clone, RUD rud, long sts);

}
