package serial;

import java.io.IOException;
import java.io.OutputStream;

public class Serializer {
	
	private final OutputStream outputStream;


	public Serializer(OutputStream outputStream) {
		// TODO Auto-generated constructor stub
		this.outputStream = outputStream;
	}
	
	public void flush() throws IOException{
		outputStream.flush();
	}
	
	public void write(byte[] response) throws IOException{
		outputStream.write(response);
	}

}
