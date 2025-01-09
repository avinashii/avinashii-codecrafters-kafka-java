package io;

import java.io.IOException;
import java.io.OutputStream;

public class DataOutputStream implements DataOutput  , AutoCloseable {
	
	private final java.io.DataOutputStream delegate;
	
	public DataOutputStream(OutputStream out) {
		this.delegate = new java.io.DataOutputStream(out);
	}

	@Override
	public void writeRawBytes(byte[] bytes) throws IOException {
		// TODO Auto-generated method stub
		
		delegate.write(bytes);
		
	}

	@Override
	public void writeByte(byte value) throws IOException {
		// TODO Auto-generated method stub
		delegate.write(value);
		
	}

	@Override
	public void writeShort(short value) throws IOException {
		// TODO Auto-generated method stub
		delegate.writeShort(value);
		
	}

	@Override
	public void writeInt(int value) throws IOException {
		// TODO Auto-generated method stub
		delegate.writeInt(value);
		
	}

	@Override
	public void writeLong(long value) throws IOException {
		// TODO Auto-generated method stub
		delegate.writeLong(value);
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		delegate.close();
		
	}
	
	

}
