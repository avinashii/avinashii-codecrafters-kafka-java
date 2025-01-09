package store;

public record Cell<T>(T value){
	
	@SuppressWarnings("unchecked")
	
	public Class<T> type(){
		return (Class<T>) value.getClass();
	}
	
	public static <T> Cell<T> with(T value){
		return new Cell<T>(value);
	}

}
