package bigdata;

import org.jboss.marshalling.Pair;
import org.redisson.Redisson;
import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.codec.DoubleCodec;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;


public class Main {
    public static void main(String[] args) {

        Config config = null;
        try {
            config = Config.fromYAML(Main.class.getClassLoader().getResource("redis-config.yml"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RedissonClient redisson = Redisson.create(config);

        try {
            List<String> query = Main.Query2(redisson, "B000F3E5OY", "01/08/2012", "31/12/1968");
            System.out.println("Query n°2 Ended:\n--------------------------");

            for (String people : query) {
                System.out.println(people);
            }
            System.out.println("--------------------------");

            List<Object> query4 = Main.Query4(redisson,query); // j'ai mit query en attendant de pouvoir avoir toute la collection
            System.out.println("Query n°4 Ended:\n--------------------------");

            for (Object people : query4) {
                System.out.println(people);
            }
            System.out.println("--------------------------");

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * Query 2:
     * For a given product during a given period, find the people who commented or
     * posted on it, and had bought it.
     * 
     * @param redisson
     * @return
     * @throws ParseException
     */
    public static List<String> Query2(RedissonClient redisson, String ProductId, String before, String after)
            throws ParseException {
        List<String> query = new ArrayList<>();
        Date beforeDate = new SimpleDateFormat("dd/MM/yyyy").parse(before);
        Date afterDate = new SimpleDateFormat("dd/MM/yyyy").parse(after);

        StringCodec codec = new StringCodec();
        RMap<String, String> map = redisson.getMap(ProductId, codec);

        List<String> clientsWithFeedBack = map.readAllKeySet().stream()
                .filter(s -> !Set.of("brand", "title", "price", "imgUrl").contains(s)).toList();

        for (String PersonId : clientsWithFeedBack) {
            RList<String> posts = redisson.getList(PersonId + "_Posts", codec);

            for (String postID : posts) {
                RMap<String, String> post = redisson.getMap(postID, codec);

                Date creationDate = new Date((int) (Float.parseFloat(post.get("creationDate"))));

                if (creationDate.before(beforeDate) && creationDate.after(afterDate)) {
                    query.add(PersonId);
                    break;
                }
            }
        }
        return query;
    }


    /**
     * Query 4. Find the top-2 persons who spend the highest amount of money in orders.
     * Then for each person, traverse her knows-graph with 3-hop to find the friends,
     * and finally return the common friends of these two persons.
     *
     * @param redisson
     * @return
     * @throws ParseException
     */
    public static List<Object> Query4(RedissonClient redisson, List<String> persons){
        String person1 = "";
        String person2 = "";
        double pricePerson1 = 0;
        double pricePerson2=0;

        StringCodec codec = new StringCodec();
        DoubleCodec codecPrice = new DoubleCodec();
        for (String personID : persons) {
            RList<String> ordersID = redisson.getList(personID+"_Orders", codec);
            double sum=0;
            for(String orderID : ordersID) {
                Double totalPrice = (Double) redisson.getMap(orderID, codecPrice).get("TotalPrice");
                sum+=totalPrice;
            }
            if (sum>pricePerson1){
                person2=person1;
                pricePerson2=pricePerson1;
                person1=personID;
                pricePerson1=sum;
            }
            else if (sum>pricePerson2){
                person2=personID;
                pricePerson2 = sum;
            }

        }

        RList<String> know1 = redisson.getList(person1 +"_Knows", codec);
        RList<String> know2 = redisson.getList(person2+"_Knows", codec);

        return know1.stream()
                .distinct()
                .filter(know2::contains)
                .collect(Collectors.toList());
    }
}


