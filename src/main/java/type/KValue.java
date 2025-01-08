package type;

import java.util.UUID;

public class KValue {
	private int apiKey;
	private int apiVersion;
	private int correlationId;
	private int errorCode;
	private KValueType type;
	private String topic;
	private UUID TopicUUID;
	
	public void setApiKey(int apiKey) {
		this.apiKey = apiKey;
		this.type = switch(this.apiKey) {
			case 18 ->KValueType.ApiVersion;
			case 75 -> KValueType.DescribeTopic;
			default ->KValueType.Unknown;
		};
	}
	
	public void setApiVersion(int apiVersion) {
		this.apiVersion = apiVersion;
		this.errorCode = (apiVersion >0 || apiVersion < 5) ? 0 : 35; 
	}
	
	public void setCorrelationId( int correlationId) {
		this.correlationId = correlationId;
	}
	
	public void setTopic(String name) {
		this.topic = name;
	}

	public void setTopicUUID(UUID TopicUUID) {
		this.TopicUUID = TopicUUID;
	}

	public String getTopic() {
        return topic;
    }

	
	public KValueType getType() {
		return type;
	}
	
	public int getCorrelationId() {
        return correlationId;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getApiKey() {
        return apiKey;
    }

    public int getApiVersion() {
        return apiVersion;
    }

    @Override
    public String toString() {
    	
    	
		return switch(type) {
			case ApiVersion ->("""
					Correlation ID: %d
					Error Code: %d
					Api Key: %d
					Api Version: %d""").formatted(correlationId ,errorCode , apiKey , apiVersion);
			case DescribeTopic ->("""
					correlation ID: %d
					Error Code: %d
					Api Key: %d
					Api Version: %d
					Topic Name: %s""").formatted(correlationId, errorCode , apiKey, apiVersion , topic);
			case Fetch-> "Fetch";
			case Unknown -> "Unknown";
			default -> "Default";
		};
    	
    }
	

}
