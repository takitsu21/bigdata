package bigdata;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import bigdata.Utils.Company;
import bigdata.Utils.RFM;

public class Main {
    public static Querys querys;
    public static Maj maj;

    public static void main(String[] args) {

        Config config = null;
        try {
            config = Config.fromYAML(Main.class.getClassLoader().getResource("redis-config.yml"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RedissonClient redisson = Redisson.create(config);

        Main.querys = new Querys(redisson);
        Main.maj = new Maj(redisson);

        //Requetes
        //testQuery1();
        // testQuery2();
        // testQuery3();
        // testQuery4();
        // testQuery5();
        // testQuery6();
        // testQuery7();
        testQuery8();
        // testQuery9();
        //testQuery10();

        //MAJ
        //maj.insert("B002NGNSOE","0000","4.0,Good");
        //maj.update("B002NGNSOE","8491","4.0,Good");
        //maj.delete("B002NGNSOE","21990232565835");

        System.exit(0);
    }

    public static void testQuery1() {
        System.out.println("Query n°1 :\n--------------------------");
        List<String> query1 = querys.Query1("2199023256013");

        for (String people : query1) {
            System.out.println(people);
        }
        System.out.println("--------------------------");
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

    public static void testQuery7() {
        System.out.println("Query n°7 :\n--------------------------");
        List<String> query7 = querys.Query7("Elfin_Sports_Cars");

        for (Object people : query7) {
            System.out.println(people);
        }
        System.out.println("--------------------------");
    }

    public static void testQuery8() {
        try {
            System.out.println("Query n°8 :\n--------------------------");
            querys.Query8("Nomis", "01/08/2020");

            System.out.println("--------------------------");
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    public static void testQuery9() {
        System.out.println("Query n°9 :\n--------------------------");
        Map<String, Company> query9 = querys.Query9("United_Kingdom");

        System.out.println(query9);
        System.out.println("--------------------------");
    }

    public static void testQuery10() {
        System.out.println("Query n°10 :\n--------------------------");
        Map<String, RFM> query10 = querys.Query10();

        System.out.println(query10);
        System.out.println("--------------------------");
    }

}
