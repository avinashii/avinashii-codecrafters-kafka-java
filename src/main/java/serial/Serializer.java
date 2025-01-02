package serial;

import type.KValue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Serializer {

    private final OutputStream outputStream;

    public Serializer(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void flush() throws IOException {
        outputStream.flush();
    }

    public void write(byte[] response) throws IOException {

        outputStream.write(response);

    }
}
