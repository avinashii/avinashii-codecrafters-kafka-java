import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    // 
     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     int port = 9092;
     try {
       serverSocket = new ServerSocket(port);
       // Since the tester restarts your program quite often, setting SO_REUSEADDR
       // ensures that we don't run into 'Address already in use' errors
       serverSocket.setReuseAddress(true);
       // Wait for connection from client.
       clientSocket = serverSocket.accept();
       InputStream in = clientSocket.getInputStream();
       OutputStream out = clientSocket.getOutputStream();
//       out.write(new byte[] {1,2,3,4});
//       out.write(new byte[] {0,0,0,7});
       
       byte[] length = in.readNBytes(4);
       byte[] apiKey = in.readNBytes(2);
       byte[] apiVersion = in.readNBytes(2);
       byte[] correlationId= in.readNBytes(4);
       
       short shortApiVersion = ByteBuffer.wrap(apiVersion).getShort();
       
       
       
       ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
       responseBuffer.put(correlationId);
       
       if(shortApiVersion < 0 || shortApiVersion > 4) {
    	   responseBuffer.put(new byte[] {0,35});
       }
       else {
    	   
    	   responseBuffer.putInt(0);
    	   
    	   
    	   responseBuffer.putShort((short) 0);
    	   
    	   responseBuffer.putInt(1);
    	   
    	   responseBuffer.putShort((short) 18);
    	   
    	   responseBuffer.putShort((short) 0);
    	   
    	   responseBuffer.putShort((short) 4);
    	          
       }
       
       int responseLength = responseBuffer.position();
       
       responseBuffer.putInt(0 ,responseLength-4);
       out.write(responseBuffer.array(),0,responseLength);


     } catch (IOException e) {
       System.out.println("IOException: " + e.getMessage());
     } finally {
       try {
         if (clientSocket != null) {
           clientSocket.close();
         }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       }
     }
  }
}
