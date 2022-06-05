package bigdata;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class Main {
    public static void main(String[] args) {

        Config config = null;
        try {
            config = Config.fromYAML(Main.class.getClassLoader().getResource("redis-config.yml"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RedissonClient redisson = Redisson.create(config);

        Querys querys = new Querys(redisson);

        testQuery2(querys);
        testQuery4(querys);

        System.exit(0);
    }

    public static void testQuery2(Querys querys) {
        try {
            System.out.println("Query n°2 :\n--------------------------");
            List<String> query2 = querys.Query2("B000F3E5OY", "01/08/2012",
                    "31/12/1968");

            for (String people : query2) {
                System.out.println(people);
            }
            System.out.println("--------------------------");
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static void testQuery4(Querys querys) {
        System.out.println("Query n°4 :\n--------------------------");
        List<String> query4 = querys.Query4();

        for (String people : query4) {
            System.out.println(people);
        }
    }

}
