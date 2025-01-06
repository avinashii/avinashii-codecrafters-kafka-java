package kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import type.KValue;

public class Kafka {
	
	public byte[] evaluate(KValue value) {
		
		return switch(value.getType()) {
			case ApiVersion -> createApiResponse(value);
			case DescribeTopic -> createDescribeResponse(value);
			case Fetch ->null;
			case Unknown -> null;
		};
		
	}

	private byte[] createDescribeResponse(KValue value) {
		// TODO Auto-generated method stub
		final var header = createHeader(value.getCorrelationId());
		final var body = createDescribeBody(value);
		
		int totalSize = header.length + 1 + body.length;
		
		ByteBuffer response = ByteBuffer.allocate(4+ totalSize);
		
		response.putInt(totalSize);
		response.put(header);
		response.put((byte) 0);
		response.put(body);
		
		return response.array();
	}

	private byte[] createDescribeBody(KValue value) {
		// TODO Auto-generated method stub
		byte[] topicBytes = value.getTopic().getBytes(StandardCharsets.UTF_8);
		
		int bodySize = 33 + topicBytes.length;
		
		ByteBuffer body  = ByteBuffer.allocate(bodySize);
		
		body.putInt(0);
		body.put((byte) 2);
		body.putShort((short) 3);
		body.put((byte)((byte) topicBytes.length+1));
		body.put(topicBytes);
		body.put(new byte[16]);
		body.put((byte) 0);
		body.put((byte) 1);
		body.putInt(0x00000DF8);
		body.put((byte) 0);
		body.put((byte) 0xFF);
		body.put((byte) 0);
		
		return body.array();
	}

	private byte[] createApiResponse(KValue value) {
		// TODO Auto-generated method stub
		
		final var header = createHeader(value.getCorrelationId());
		final var body = createBody(value);
		
		int totalSize= header.length + body.length;
		
		ByteBuffer response = ByteBuffer.allocate(totalSize+4);
		
		response.putInt(totalSize);
		response.put(header);
		response.put(body);
		
		return response.array();
	}

	private byte[] createBody(KValue value) {

		
		final var errorCode = value.getErrorCode();
		
		ByteBuffer body = ByteBuffer.allocate(28);
		
		body.putShort((short) errorCode);
		
		if(errorCode == 0) {
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
			body.putInt((byte) 0);
			
		}
		int position = body.position();
		byte[] bodyByte = new byte[position];
		body.flip();
		body.get(bodyByte);
		
		return bodyByte;		
		}
	
	
	private byte[] createHeader(int correlationId) {
		// TODO Auto-generated method stub
		ByteBuffer header = ByteBuffer.allocate(4);
		header.putInt(correlationId);
		return header.array();

		}

}
