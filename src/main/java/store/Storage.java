package store;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class Storage {

    private final Map<String, Cell<Object>> map = new ConcurrentHashMap<>();

    public void clear() {
        map.clear();
    }

    public void set(String key, Object value) {
        map.put(key, Cell.with(value));
    }

    public void put(String key, Cell<Object> cell) {
        map.put(key, cell);
    }

    public Object get(String key) {
        System.out.println("key = " + key);
        System.out.println("map = " + map);

        final var cell = map.get(key);

        if (cell != null) return cell.value();

        return null;
    }

}
