package kafka;

import type.KValue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Kafka {

    public byte[] evaluate(KValue value) {

        return switch (value.getType()) {
            case ApiVersion -> createApiResponse(value);
            case DescribeTopic -> createDescribeResponse(value);
            case Fetch -> null;
            case Unknown -> null;
        };
    }

    private byte[] createDescribeResponse(KValue value) {
    	final var header = createHeader(value.getCorrelationId());
        
        // Parse the metadata log file
        File metadataLogFile = new File("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
        ClusterMetadataParser.TopicMetadata topicMetadata;

        try {
            Map<String, ClusterMetadataParser.TopicMetadata> metadataMap = ClusterMetadataParser.parseClusterMetadata(metadataLogFile);
            topicMetadata = metadataMap.get(value.getTopic());
            if (topicMetadata == null) {
                throw new IOException("Topic not found: " + value.getTopic());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse cluster metadata", e);
        }

        // Build the response body
        byte[] body = DescribeTopicPartitionsHandler.buildDescribeTopicPartitionsResponse(
            value.getCorrelationId(),
            value.getTopic(),
            topicMetadata
        );

        // Combine header and body into the final response
        int totalSize = header.length + body.length + 1; // +1 for tag buffer
        ByteBuffer response = ByteBuffer.allocate(4 + totalSize);

        response.putInt(totalSize);
        response.put(header);
        response.put((byte) 0); // tag buffer
        response.put(body);

        return response.array();
    }

    private byte[] createApiResponse(KValue value) {
        final var header = createHeader(value.getCorrelationId());
        final var body = createBody(value);

        int totalSize = header.length + body.length;
        ByteBuffer response = ByteBuffer.allocate(4 + totalSize);

        response.putInt(totalSize);
        response.put(header);
        response.put(body);

        return response.array();
    }

    private byte[] createHeader(int correlationId) {
        ByteBuffer header = ByteBuffer.allocate(4);
        header.putInt(correlationId);

        return header.array();
    }

    private byte[] createDescribeBody(KValue value) {
        byte[] topicBytes = value.getTopic().getBytes(StandardCharsets.UTF_8);

        int bodySize = 33 + topicBytes.length;
        ByteBuffer body = ByteBuffer.allocate(bodySize);
                body.putInt(0); // throttle time 4 bytes
        body.put((byte) 2); // array length 1 byte
        body.putShort((short) 0); // errorCode 2 bytes
        body.put((byte) ((byte) topicBytes.length + 1)); // 1 byte
        body.put(topicBytes); // topic length bytes
        body.put((byte) 0); // 1 byte
        body.put((byte) 1); // 1 byte
        body.putInt(0x00000DF8); // 4 bytes
        body.put((byte) 0); // tag buffer 1 byte
        body.put((byte) 0xFF); // cursor 1 byte
        body.put((byte) 0); // tag buffer 1 byte

        return body.array();
    }

    private byte[] createBody(KValue value) {
        final var errorCode = value.getErrorCode();

        ByteBuffer body = ByteBuffer.allocate(28);
        body.putShort((short) errorCode);

        if (errorCode == 0) {
            final var apiKey = value.getApiKey();
            final var apiVersion = value.getApiVersion();

            body.put((byte) 3);
            body.putShort((short) apiKey);
            body.putShort((short) 2);
            body.putShort((short) apiVersion);
            body.put((byte) 0);
            body.putShort((short) 75);
            body.putShort((short) 0);
            body.putShort((short) 0);
            body.put((byte) 0);
            body.putInt(0);
            body.put((byte) 0);
        }

        int position = body.position();
        byte[] bodyBytes = new byte[position];
        body.flip();
        body.get(bodyBytes);

        return bodyBytes;
    }
    public class ClusterMetadataParser {

        public static Map<String, TopicMetadata> parseClusterMetadata(File logFile) throws IOException {
            Map<String, TopicMetadata> metadataMap = new HashMap<>();

            try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(","); // Adjust format based on actual log file structure
                    String topicName = parts[0];
                    UUID topicUUID = UUID.fromString(parts[1]);
                    int partitionCount = Integer.parseInt(parts[2]);

                    metadataMap.put(topicName, new TopicMetadata(topicUUID, partitionCount));
                }
            }

            return metadataMap;
        }

        public static class TopicMetadata {
            private final UUID topicUUID;
            private final int partitionCount;

            public TopicMetadata(UUID topicUUID, int partitionCount) {
                this.topicUUID = topicUUID;
                this.partitionCount = partitionCount;
            }

            public UUID getTopicUUID() {
                return topicUUID;
            }

            public int getPartitionCount() {
                return partitionCount;
            }
        }
    }
  

    public class DescribeTopicPartitionsHandler {

        public static byte[] buildDescribeTopicPartitionsResponse(int correlationId, String topicName, ClusterMetadataParser.TopicMetadata metadata) {
            ByteArrayOutputStream responseStream = new ByteArrayOutputStream();

            try (DataOutputStream dataOut = new DataOutputStream(responseStream)) {
                // Write response header
                dataOut.writeInt(0); // Placeholder for message length
                dataOut.writeInt(correlationId); // Correlation ID

                // Write response body
                dataOut.writeShort(0); // Error code (0 = success)
                dataOut.writeUTF(topicName); // Topic name
                dataOut.writeLong(metadata.getTopicUUID().getMostSignificantBits());
                dataOut.writeLong(metadata.getTopicUUID().getLeastSignificantBits());

                // Partition information
                dataOut.writeInt(metadata.getPartitionCount()); // Partition count (assume 1 for this example)
                dataOut.writeInt(0); // Partition index
                dataOut.writeShort(0); // Partition error code

                // Update message length
                byte[] responseBytes = responseStream.toByteArray();
                int messageLength = responseBytes.length - 4;
                System.arraycopy(ByteBuffer.allocate(4).putInt(messageLength).array(), 0, responseBytes, 0, 4);

                return responseBytes;
            } catch (IOException e) {
                throw new RuntimeException("Failed to build DescribeTopicPartitions response", e);
            }
        }

        public static void handleDescribeTopicPartitionsRequest(InputStream requestStream, OutputStream responseStream, File metadataLogFile) throws IOException {
            DataInputStream dataIn = new DataInputStream(requestStream);

            int correlationId = dataIn.readInt();
            String topicName = dataIn.readUTF();

            Map<String, ClusterMetadataParser.TopicMetadata> metadataMap = ClusterMetadataParser.parseClusterMetadata(metadataLogFile);
            ClusterMetadataParser.TopicMetadata metadata = metadataMap.get(topicName);

            if (metadata == null) {
                throw new IOException("Topic not found: " + topicName);
            }

            byte[] response = buildDescribeTopicPartitionsResponse(correlationId, topicName, metadata);
            responseStream.write(response);
        }
    }


}