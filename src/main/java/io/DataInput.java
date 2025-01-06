package io;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface DataInput {
	
	ByteBuffer readNBytes(int n) throws IOException;
	
	byte peekByte() throws IOException;

    byte readSignedByte() throws IOException;

    short readSignedShort() throws IOException;

    int readSignedInt() throws IOException;

    long readSignedLong() throws IOException;

}
