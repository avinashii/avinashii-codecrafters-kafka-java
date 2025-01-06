package serial;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import type.KValue;

public class Deserializer {
	
	private final DataInputStream inputStream;
	private int messageSize;

	public Deserializer(InputStream inputStream) {
		// TODO Auto-generated constructor stub
		this.inputStream = new DataInputStream(inputStream);
	}
	
	
	public KValue read() throws IOException{
		
		var value = new KValue();
		
		this.messageSize = inputStream.readInt();
		System.out.println("messageSize : "+ messageSize);
		
		parseHeader(value);
		
		return switch(value.getType()) {
			case ApiVersion->parserApiVersion(value);
			case DescribeTopic -> parserDescribeTopic(value);
			case Fetch ->null;
			case Unknown ->value;
		};
		
	}

	private KValue parserApiVersion(KValue value) throws IOException {
		// TODO Auto-generated method stub
		inputStream.readNBytes(messageSize);
		return value;
	}

	private KValue parserDescribeTopic(KValue value) throws IOException {
		// TODO Auto-generated method stub
		final var arrayLength = readLength()-1;
		
		final var topicName = readString();
	
		inputStream.readByte();
		
		final var paritionLimit = inputStream.readInt();
		inputStream.readByte();
		inputStream.readByte();
		
		value.setTopic(topicName);
		
		return value;
		
	}




	private String readString() throws IOException {
		// TODO Auto-generated method stub
		final var length = readLength()-1;
		
		final var content = inputStream.readNBytes(length);
		
		return new String(content , StandardCharsets.US_ASCII);
	}


	private int readLength() throws IOException {
        final var first = readUnsignedByte();

        return first & 0b0011_1111;
    }


	private int readUnsignedByte()throws IOException {
		// TODO Auto-generated method stub
		return Byte.toUnsignedInt(inputStream.readByte());
	}


	private void parseHeader(KValue value) throws IOException {
		// TODO Auto-generated method stub
		final int apiKey = inputStream.readShort();
		final int apiVersion = inputStream.readShort();
		final int correlationId = inputStream.readInt();
		final int clientLength = inputStream.readShort();
		final var clientId = inputStream.readNBytes(clientLength);
		inputStream.readByte();
		
		messageSize = messageSize - 11 - clientLength;
		
		value.setApiKey(apiKey);
		value.setApiVersion(apiVersion);
		value.setCorrelationId(correlationId);
		
		
	}

}
