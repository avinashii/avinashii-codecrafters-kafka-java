package io;

import java.io.IOException;

public interface DataOutput {
	
	void writeRawBytes(byte[] bytes) throws IOException;
	
	void writeByte(byte value) throws IOException;
	
	void writeShort(short value) throws IOException;
	
	void writeInt(int value) throws IOException;
	
	void writeLong(long value) throws IOException;
	
	default void writeBoolean(boolean value) throws IOException	{
		writeByte((byte) (value?1:0));
	}
	

}
