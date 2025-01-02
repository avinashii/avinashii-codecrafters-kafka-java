package kafka;

import type.KValue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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
        final var body = createDescribeBody(value);

        int totalSize = header.length + 1 + body.length;
        ByteBuffer response = ByteBuffer.allocate(4 + totalSize);

        response.putInt(totalSize);
        response.put(header);
        response.put((byte) 0); // tab buffer
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
        body.putShort((short) 3); // errorCode 2 bytes
        body.put((byte) ((byte) topicBytes.length + 1)); // 1 byte
        body.put(topicBytes); // topic length bytes
        body.put(new byte[16]); // 16 bytes
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

}
