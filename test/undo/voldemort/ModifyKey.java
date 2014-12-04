package voldemort;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.undoTracker.DBProxy;
import voldemort.utils.ByteArray;

public class ModifyKey {

    private static final double MULTIPLICATION_FACTOR = getMultiplicationFactor();
    private static final int TIMESTAMP_SIZE = 16;
    private static long lastTime;

    @Test
    public void testModifyKey() {
        ByteArray originalKey = new ByteArray("vamos".getBytes());
        ByteArray key = originalKey.clone();
        Long ts = getTimestamp();
        DBProxy.modifyKey(key, ts);
        DBProxy.removeKeyVersion(key);
        Assert.assertTrue(Arrays.equals(originalKey.get(), key.get()));
    }

    private static synchronized long getTimestamp() {
        // TODO this may have a strong performance impact
        long time = (long) (System.currentTimeMillis() * MULTIPLICATION_FACTOR);
        if(time <= lastTime) {
            time = (lastTime + 1);
        }
        lastTime = time;
        return time;
    }

    private static double getMultiplicationFactor() {
        long x = System.currentTimeMillis();
        int digits = countDigits(x);
        // target is 16 digits
        double diff = Math.pow(10, TIMESTAMP_SIZE - digits);
        x = (long) (x * diff);
        if(countDigits(x) != TIMESTAMP_SIZE) {
            throw new RuntimeException("The timestamp has not 16 digits");
        }
        return diff;
    }

    private static int countDigits(long v) {
        int i = 1;
        while(v >= 10) {
            v = v / 10;
            i++;
        }
        return i;
    }
}
