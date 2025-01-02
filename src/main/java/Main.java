import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {
        int port = 9092;
        System.err.println("Logs from your program will appear here!");

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    handleClient(clientSocket);
                } catch (IOException e) {
                    System.err.println("Error handling client: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) throws IOException {
        InputStream in = clientSocket.getInputStream();
        OutputStream out = clientSocket.getOutputStream();

        while (true) {
            try {
                // Read request
                byte[] lengthBytes = in.readNBytes(4);
                int length = ByteBuffer.wrap(lengthBytes).getInt();

                byte[] apiKeyBytes = in.readNBytes(2);
                byte[] apiVersionBytes = in.readNBytes(2);
                byte[] correlationIdBytes = in.readNBytes(4);

                short apiVersion = ByteBuffer.wrap(apiVersionBytes).getShort();

                // Prepare response
                var bos = new ByteArrayOutputStream();
                bos.write(correlationIdBytes);

                if (apiVersion < 0 || apiVersion > 4) {
                    bos.write(new byte[]{0, 35}); // Error: unsupported version
                } else {
                    bos.write(new byte[]{0, 0}); // No error
                    bos.write(1);               // Number of APIs (1)

                    // API key entry for ApiVersions
                    bos.write(new byte[]{0, 18}); // ApiKey 18
                    bos.write(new byte[]{0, 3});  // MinVersion
                    bos.write(new byte[]{0, 4});  // MaxVersion

                    bos.write(0);               // Tagged fields (empty)
                }

                // Add response size
                int size = bos.size();
                byte[] sizeBytes = ByteBuffer.allocate(4).putInt(size).array();

                // Write response
                out.write(sizeBytes);
                out.write(bos.toByteArray());
                out.flush();
            } catch (IOException e) {
                System.err.println("Client disconnected or error: " + e.getMessage());
                break; // Exit the loop if client disconnects
            }
        }
    }
}
