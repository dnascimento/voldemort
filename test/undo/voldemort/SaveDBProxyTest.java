package voldemort;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.ShuttleShutdownProcess;
import voldemort.undoTracker.DBProxyFactory;

public class SaveDBProxyTest {

    String OUTPUT_FILE = "save.test";

    @Test
    public void testSave() {
        DBProxy proxy = new DBProxy();
        ShuttleShutdownProcess saver = new ShuttleShutdownProcess(true, true, proxy, OUTPUT_FILE);
        saver.save();
        DBProxy proxyFromFile = DBProxyFactory.loadDBProxyFromFile(OUTPUT_FILE);
        Assert.assertEquals(proxy, proxyFromFile);
    }
}
