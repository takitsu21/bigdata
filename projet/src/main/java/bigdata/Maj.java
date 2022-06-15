package bigdata;

import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Maj {
    public final RedissonClient redisson;
    public final StringCodec codecString = new StringCodec();

    public Maj(RedissonClient redisson) {
        this.redisson = redisson;
    }

    public void insertMany(String collection, HashMap<String, String> allValues) {
        RMap<String, String> map = redisson.getMap(collection, codecString);
        map.putAll(allValues);
        for (Map.Entry<String, String> entry : allValues.entrySet()) {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }
    }

    public void insert(String collection, String key, String value) {
        HashMap<String, String> toInsert = new HashMap<>();
        toInsert.put(key, value);
        insertMany(collection, toInsert);
    }

    public void updateMany(String collection, HashMap<String, String> allValues) {
        RMap<String, String> map = redisson.getMap(collection, codecString);
        map.replaceAll((k, v) -> {
            if (allValues.containsKey(k)) {
                System.out.println(k + "/" + allValues.get(k));
                return allValues.get(k);
            }
            return v;
        });
    }

    public void update(String collection, String key, String newValue) {
        HashMap<String, String> toInsert = new HashMap<>();
        toInsert.put(key, newValue);
        updateMany(collection, toInsert);
    }

    public void deleteMany(String collection, List<String> keys) {
        RMap<String, String> map = redisson.getMap(collection, codecString);
        keys.forEach(key -> {
            map.remove(key);
            System.out.println(key + " is removed : " + !map.containsKey(key));
        });
    }

    public void delete(String collection, String key) {
        this.deleteMany(collection, List.of(key));
    }
}
