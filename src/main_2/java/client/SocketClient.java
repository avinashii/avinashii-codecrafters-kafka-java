package client;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.Kafka;
import serial.Deserializer;
import serial.Serializer;

public class SocketClient implements Runnable {

	
	private static final AtomicInteger ID_INCREMENT  = new AtomicInteger();
	
	private final int id;
	private final Socket socket;
	private final Kafka evaluator;
	private boolean connected;
	
	
	
	public SocketClient(Socket socket, Kafka evaluator) {
		// TODO Auto-generated constructor stub
		this.id = ID_INCREMENT.incrementAndGet();
		this.socket = socket;
		this.evaluator = evaluator;
	}

	
	@Override
	public void run() {
		
		
		connected = true;
		System.out.println("%d: connected".formatted(id));
		
		try(socket){
			
			final var inputStream = socket.getInputStream();
			final var outputStream = socket.getOutputStream();
			
			final var deserializer = new Deserializer(inputStream);
			final var serializer = new Serializer(outputStream);
			
			while(true) {
				 final var request = deserializer.read();
				 
				 if(request == null) return;
				 System.out.println("Received: " + request);
				 
				 final var response = evaluator.evaluate(request);
				 
				 serializer.write(response);
				 outputStream.flush();
			}
		}catch(IOException E) {
			System.out.println("error : "+ E.getMessage());	
		}
		
	}

}
