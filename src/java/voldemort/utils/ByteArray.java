package voldemort.utils;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * A byte array container that provides an equals and hashCode pair based on the
 * contents of the byte array. This is useful as a key for Maps.
 */
public final class ByteArray implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final ByteArray EMPTY = new ByteArray();

    private byte[] underlying;

    public ByteArray(byte... underlying) {
        this.underlying = Utils.notNull(underlying, "underlying");
    }

    public byte[] get() {
        return underlying;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(underlying);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(!(obj instanceof ByteArray))
            return false;
        ByteArray other = (ByteArray) obj;
        return Arrays.equals(underlying, other.underlying);
    }

    @Override
    public String toString() {
        return ByteUtils.toHexString(underlying);
    }

    /**
     * Translate the each ByteArray in an iterable into a hexidecimal string
     * 
     * @param arrays The array of bytes to translate
     * @return An iterable of converted strings
     */
    public static Iterable<String> toHexStrings(Iterable<ByteArray> arrays) {
        ArrayList<String> ret = new ArrayList<String>();
        for(ByteArray array: arrays)
            ret.add(ByteUtils.toHexString(array.get()));
        return ret;
    }

    public int length() {
        return underlying.length;
    }

    /**
     * Returns a new container object but sharing the byte array
     * 
     * @return
     */
    public ByteArray shadow() {
        return new ByteArray(underlying);
    }

    /**
     * Clone the array copying its content. For performance concerns, check if
     * shadow fits your requirements
     */
    public ByteArray clone() {
        byte[] newArray = new byte[underlying.length];
        for(int i = 0; i < newArray.length; i++) {
            newArray[i] = underlying[i];
        }
        return new ByteArray(newArray);
    }

    public void set(byte[] data) {
        this.underlying = Utils.notNull(data, "underlying");
    }

    public static String toAscii(ByteArray key) {
        try {
            return new String(key.get(), "UTF-8");
        } catch(UnsupportedEncodingException e) {}
        return key.toString();
    }
}
