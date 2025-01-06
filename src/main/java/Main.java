
import kafka.Kafka;
import kafka.KafkaLoader;
import store.Storage;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadFactory;

import client.SocketClient;


public class Main {

    public static void main(String[] args) throws IOException {
    	
    	final ThreadFactory threadFactory = Thread.ofVirtual().factory();
    	
    	final Storage storage= new Storage();
    	
    	final Kafka kafka = new Kafka();
    	
    	Path path = null;
    	
    	if(args.length == 1) {
    		
    		System.out.println(args[0]);
    		
    		path = Paths.get(args[0]);
    		
    		if(Files.exists(path)) {
    			KafkaLoader.load(path , storage);
    		}
    	}
    	
    	final int port = 9092;
    	
    	System.out.println("port: %s".formatted(port));

    	
    	try(final ServerSocket serverSocket = new ServerSocket(port)){
    		serverSocket.setReuseAddress(true);
    			
    			while(true) {
    				final Socket socket = serverSocket.accept();
    				
    				final var client = new SocketClient(socket , kafka);
    				
    				final Thread thread = threadFactory.newThread(client);
    				thread.start();
    			}
    	}
    
    }
    

}
