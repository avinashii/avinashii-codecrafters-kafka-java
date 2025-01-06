package kafka;

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
		return null;
	}

	private byte[] createApiResponse(KValue value) {
		// TODO Auto-generated method stub
		return null;
	}

}
