package io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DataInputStream implements DataInput {
	
	private final java.io.DataInputStream delegate;
	
	public DataInputStream(InputStream in) {
		this.delegate = new java.io.DataInputStream(in);
	}

	@Override
	public ByteBuffer readNBytes(int n) throws IOException {
		// TODO Auto-generated method stub
		return ByteBuffer.wrap(delegate.readNBytes(n));
		
	}

	@Override
	public byte peekByte() throws IOException {
		// TODO Auto-generated method stub
		delegate.mark(1);
		byte value = delegate.readByte();
		delegate.reset();
		return value;
	}

	@Override
	public byte readSignedByte() throws IOException {
		// TODO Auto-generated method stub
		return delegate.readByte();
	}

	@Override
	public short readSignedShort() throws IOException {
		// TODO Auto-generated method stub
		return delegate.readShort();
	}

	@Override
	public int readSignedInt() throws IOException {
		// TODO Auto-generated method stub
		return delegate.readInt();
	}

	@Override
	public long readSignedLong() throws IOException {
		// TODO Auto-generated method stub
		return delegate.readLong();
	}

}
