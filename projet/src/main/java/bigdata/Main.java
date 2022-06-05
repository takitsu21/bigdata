package bigdata;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Set;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class Main {
    public static Querys querys;

    public static void main(String[] args) {

        Config config = null;
        try {
            config = Config.fromYAML(Main.class.getClassLoader().getResource("redis-config.yml"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RedissonClient redisson = Redisson.create(config);

        Main.querys = new Querys(redisson);

        // testQuery2();
        // testQuery3();
        // testQuery4();
        // testQuery5();
        testQuery6();

        System.exit(0);
    }

    public static void testQuery2() {
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

    public static void testQuery3() {
        try {
            System.out.println("Query n°3 :\n--------------------------");
            List<String> query3 = querys.Query3("B000F3E5OY", "01/08/2012", "31/12/1968");

            for (String content : query3) {
                System.out.println(content);
            }
            System.out.println("--------------------------");
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    public static void testQuery4() {
        System.out.println("Query n°4 :\n--------------------------");
        List<String> query4 = querys.Query4();

        for (String people : query4) {
            System.out.println(people);
        }
    }

    public static void testQuery5() {
        System.out.println("Query n°5 :\n--------------------------");
        Set<String> query5 = querys.Query5("4145", "Nomis");

        for (Object people : query5) {
            System.out.println(people);
        }
        System.out.println("--------------------------");
    }

    public static void testQuery6() {
        System.out.println("Query n°6 :\n--------------------------");
        List<String> query6 = querys.Query6("4149", "10995116280950");

        for (Object people : query6) {
            System.out.println(people);
        }
        System.out.println("--------------------------");
    }

}
