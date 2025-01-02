package util;

import java.io.IOException;
import java.io.OutputStream;

public class TrackedOutputStream extends OutputStream {

    private final OutputStream delegate;
    private long written;

    public TrackedOutputStream(OutputStream outputStream) throws IOException {
        this.delegate = outputStream;
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
        written++;
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        delegate.write(bytes);
        written += bytes.length;
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    public void begin() {
        written = 0;
    }

    public long count() {
        return written;
    }
}
