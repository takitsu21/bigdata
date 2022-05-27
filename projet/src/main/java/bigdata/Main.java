package bigdata;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        Config config = Config.fromYAML(Main.class.getClassLoader().getResource("redis-config.yml"));

        RedissonClient redisson = Redisson.create(config);

        Iterable<String> keys = redisson.getKeys().getKeys();
        for (String s : keys) {
            System.out.println(s);
        }
    }
}
