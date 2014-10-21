package voldemort;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.SaveKeyAccess;
import voldemort.undoTracker.UndoLoad;

public class SaveDBProxyTest {

    String OUTPUT_FILE = "save.test";

    @Test
    public void testSave() {
        DBProxy proxy = new DBProxy();
        SaveKeyAccess saver = new SaveKeyAccess(true, true, proxy, OUTPUT_FILE);
        saver.save();
        DBProxy proxyFromFile = UndoLoad.loadDBProxyFromFile(OUTPUT_FILE);
        Assert.assertEquals(proxy, proxyFromFile);
    }
}
