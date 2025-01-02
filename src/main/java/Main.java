import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

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
       
       byte buffer[] = new byte[1024];
       int byteread;
       
       if((byteread = in.read(buffer)) != -1) {
    	   byte[] output = new byte[] {0,0,0,0,0,0,0,0};
    	   System.arraycopy(buffer,8, output, 4, 4);
    	   out.write(output);
       }
       else {
    	   System.out.println("Nothing to read froom intput stream ");
       }


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
