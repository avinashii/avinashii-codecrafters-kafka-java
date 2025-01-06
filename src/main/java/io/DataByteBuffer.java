package io;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DataByteBuffer implements DataInput {
	
	private final ByteBuffer delegate;
	
	public DataByteBuffer(ByteBuffer buffer) {
		this.delegate =	buffer;
	}

	@Override
	public ByteBuffer readNBytes(int n) throws IOException {
		// TODO Auto-generated method stub
		
		final var bytes = new byte[n];
		
		delegate.get(bytes, 0, bytes.length);
		
		
		return ByteBuffer.wrap(bytes);
	}

	@Override
	public byte peekByte() throws IOException {
		// TODO Auto-generated method stub
		
		delegate.mark();
		byte value = delegate.get();
		delegate.reset();
		
		return value;
	}

	@Override
	public byte readSignedByte() throws IOException {
		// TODO Auto-generated method stub
		return delegate.get();
	}

	@Override
	public short readSignedShort() throws IOException {
		// TODO Auto-generated method stub
		return delegate.getShort();
	}

	@Override
	public int readSignedInt() throws IOException {
		// TODO Auto-generated method stub
		return delegate.getInt();
	}

	@Override
	public long readSignedLong() throws IOException {
		// TODO Auto-generated method stub
		return delegate.getLong();
	}
	
	

}
