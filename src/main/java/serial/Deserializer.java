	package serial;

import type.KValue;
import type.KValueType;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;


public class Deserializer {

    private final DataInputStream inputStream;
    private int messageSize;

    public Deserializer(InputStream inputStream) {
        this.inputStream = new DataInputStream(inputStream);
    }

    public KValue read() throws IOException {
        var value = new KValue();

        this.messageSize = inputStream.readInt();
        System.out.println("messageSize = " + messageSize);

        parseHeader(value);

        return switch (value.getType()) {
            case ApiVersion -> parseApiVersion(value);
            case DescribeTopic -> parseDescribeTopic(value);
            case Fetch -> null;
            case Unknown -> value;
        };
    }

    private KValue parseApiVersion(KValue value) throws IOException {

        inputStream.readNBytes(messageSize);

        return value;
    }

    private KValue parseDescribeTopic(KValue value) throws IOException {
        final var arrayLength = readLength() - 1;
        final var topicName = readString();
        
        inputStream.readByte(); // skip tag buffer

        final var partitionLimit = inputStream.readInt();
        
        long mostSignificantBits = inputStream.readLong();
        long leastSignificantBits = inputStream.readLong();
        UUID topicUUID = new UUID(mostSignificantBits, leastSignificantBits);


//        System.out.println("array length = " + arrayLength);
//        System.out.println("topicName = " + topicName);
//        System.out.println("partitionLimit = " + partitionLimit);

        inputStream.readByte(); // skip cursor
        inputStream.readByte(); // skip tag buffer

        value.setTopic(topicName);
        value.setTopicUUID(topicUUID);
        
        return value;
    }

    private void parseHeader(KValue value) throws IOException {
        final int apiKey = inputStream.readShort(); // 2 bytes
        final int apiVersion = inputStream.readShort(); // 2 bytes
        final int correlationId = inputStream.readInt(); // 4 bytes
        final int clientLength = inputStream.readShort(); // 2 bytes
        final var clientId = inputStream.readNBytes(clientLength);
        inputStream.readByte(); // skip tag buffer

        messageSize = messageSize - 11 - clientLength;

        value.setApiKey(apiKey);
        value.setApiVersion(apiVersion);
        value.setCorrelationId(correlationId);
    }

    private String readString() throws IOException {
        final var length = readLength() - 1;

        //System.out.println("length = " + length);

        final var content = inputStream.readNBytes(length);
        return new String(content, StandardCharsets.US_ASCII);
    }

    private int readUnsignedByte() throws IOException {
        return Byte.toUnsignedInt(inputStream.readByte());
    }

    private int readLength() throws IOException {
        final var first = readUnsignedByte();

        return first & 0b0011_1111;
    }
}