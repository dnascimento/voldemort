package voldemort.store.compress;

import java.io.IOException;

/**
 * Implementation of CompressionStrategy that returns the original data
 * unchanged. A typical use-case for this is not to compress the keys when using
 * {@link CompressingStore}.
 */
public class NoopCompressionStrategy implements CompressionStrategy {

    @Override
    public byte[] deflate(byte[] data) throws IOException {
        return data;
    }

    @Override
    public byte[] inflate(byte[] data) throws IOException {
        return data;
    }

    @Override
    public String getType() {
        return "noop";
    }
}
