import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            System.out.println("Server is listening on port " + port);

            // Wait for connection from client
            clientSocket = serverSocket.accept();
            System.out.println("Client connected: " + clientSocket.getInetAddress());

            try (InputStream in = clientSocket.getInputStream();
                 OutputStream out = clientSocket.getOutputStream()) {

                while (true) {
                    byte[] length = in.readNBytes(4);
                    if (length.length < 4) {
                        System.out.println("Incomplete request length received. Closing connection.");
                        break;
                    }

                    byte[] apiKey = in.readNBytes(2);
                    byte[] apiVersion = in.readNBytes(2);
                    byte[] correlationId = in.readNBytes(4);

                    short shortApiVersion = ByteBuffer.wrap(apiVersion).getShort();

                    var bos = new ByteArrayOutputStream();
                    bos.write(correlationId);

                    if (shortApiVersion < 0 || shortApiVersion > 4) {
                        bos.write(new byte[]{0, 35});
                    } else {
                        bos.write(new byte[]{0, 0});
                        bos.write(2);
                        bos.write(new byte[]{0, 18});
                        bos.write(new byte[]{0, 3});
                        bos.write(new byte[]{0, 4});
                        bos.write(0);
                        bos.write(new byte[]{0, 0, 0, 0});
                        bos.write(0);
                    }

                    int size = bos.size();
                    byte[] sizeByte = ByteBuffer.allocate(4).putInt(size).array();
                    byte[] response = bos.toByteArray();

                    System.out.println("Sending response:");
                    System.out.println(Arrays.toString(sizeByte));
                    System.out.println(Arrays.toString(response));

                    out.write(sizeByte);
                    out.write(response);
                    out.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException during cleanup: " + e.getMessage());
            }
        }
    }
}
