package voldemort.store.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;

import voldemort.annotations.Experimental;

/**
 * Implementation of CompressionStrategy for the Snappy format. Snappy is optimized
 * for speed.
 *
 * TODO Use block encoding instead of streams for better performance, see:
 * https://github.com/dain/snappy/issues/4. Also be aware that the stream format may not be finalised
 * yet.
 */
@Experimental
public class SnappyCompressionStrategy extends StreamCompressionStrategy {

    @Override
    public String getType() {
        return "snappy";
    }

    @Override
    protected OutputStream wrapOutputStream(OutputStream underlying) throws IOException {
        return new SnappyOutputStream(underlying);
    }

    @Override
    protected InputStream wrapInputStream(InputStream underlying) throws IOException {
        return new SnappyInputStream(underlying);
    }

}
