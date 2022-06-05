package bigdata;

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
            List<String> query2 = Main.Query2(redisson, "B000F3E5OY", "01/08/2012", "31/12/1968");
            System.out.println("Query n°2 Ended:\n--------------------------");

            for (String people : query2) {
                System.out.println(people);
            }
            System.out.println("--------------------------");
            System.out.println("Query n°3 Ended:\n--------------------------");

            List<String> query3 = Main.Query3(redisson, "B000F3E5OY", "01/08/2012", "31/12/1968");
            for (String content : query3) {
                System.out.println(content);
            }
            System.out.println("--------------------------");

            List<String> query4 = Main.Query4(redisson);
            System.out.println("Query n°4 Ended:\n--------------------------");
            System.out.println("SIZE: " + query4.size());
            System.out.println("--------------------------");

        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.exit(0);
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
     * Query 3. For a given product during a given period, find people who have undertaken
     * activities related to it, e.g., posts, comments, and review, and return sentences from these texts
     * that contain negative sentiments
     * @param redisson
     * @param ProductId
     * @param before
     * @param after
     * @return
     * @throws ParseException
     */
    public static List<String> Query3(RedissonClient redisson, String ProductId, String before, String after)
            throws ParseException {
        List<String> query = new ArrayList<>();
        Date beforeDate = new SimpleDateFormat("dd/MM/yyyy").parse(before);
        Date afterDate = new SimpleDateFormat("dd/MM/yyyy").parse(after);

        StringCodec codec = new StringCodec();
        RMap<String, String> map = redisson.getMap(ProductId, codec);

        List<String> clientsWithFeedBack = map.readAllKeySet().stream()
                .filter(s -> !Set.of("brand", "title", "price", "imgUrl").contains(s)).toList();
        System.out.println(clientsWithFeedBack);
        for (String PersonId : clientsWithFeedBack) {
            RList<String> posts = redisson.getList(PersonId + "_Posts", codec);
            String[] splitReview = map.get(PersonId).split(",");
            float rate = Float.parseFloat(splitReview[0].replace("'", ""));
            String review = String.join("", Arrays.copyOfRange(splitReview, 1, splitReview.length));

            for (String postID : posts) {
                RMap<String, String> post = redisson.getMap(postID, codec);

                Date creationDate = new Date((int) (Float.parseFloat(post.get("creationDate"))));

                // rate < 2.5 = review negative
                if (creationDate.before(beforeDate) && creationDate.after(afterDate) && rate < 2.5) {
                    query.add(review);
                    break;
                }
            }
        }
        return query;
    }

    /**
     * Query 4. Find the top-2 persons who spend the highest amount of money in
     * orders.
     * Then for each person, traverse her knows-graph with 3-hop to find the
     * friends,
     * and finally return the common friends of these two persons.
     *
     * @param redisson
     * @return
     * @throws ParseException
     */
    public static List<String> Query4(RedissonClient redisson) {
        String person1 = "";
        String person2 = "";
        double pricePerson1 = 0;
        double pricePerson2 = 0;

        StringCodec codecString = new StringCodec();
        DoubleCodec codecPrice = new DoubleCodec();

        RList<String> persons = redisson.getList("Customers", codecString);
        for (String personID : persons) {
            RList<String> ordersID = redisson.getList(personID + "_Orders", codecString);
            double sum = 0;
            for (String orderID : ordersID) {
                Double totalPrice = (Double) redisson.getMap(orderID, codecPrice).get("TotalPrice");
                sum += totalPrice;
            }
            if (sum > pricePerson1) {
                person2 = person1;
                pricePerson2 = pricePerson1;
                person1 = personID;
                pricePerson1 = sum;
            } else if (sum > pricePerson2) {
                person2 = personID;
                pricePerson2 = sum;
            }

        }

        Set<String> hopThreeP1 = Main.getHopThree(redisson, person1, 3, new HashSet<>());
        Set<String> hopThreeP2 = Main.getHopThree(redisson, person2, 3, new HashSet<>());

        return hopThreeP1.stream()
                .distinct()
                .filter(hopThreeP2::contains)
                .collect(Collectors.toList());
    }

    public static Set<String> getHopThree(RedissonClient redisson, String personId, int depth, Set<String> seen) {
        StringCodec codec = new StringCodec();
        if (depth == 1) {
            return new HashSet<>(redisson.getList(personId + "_Knows", codec));
        }
        RList<String> knows = redisson.getList(personId + "_Knows", codec);
        Set<String> threeHop = new HashSet<>(knows);
        for (String person : knows) {
            if (seen.contains(person)) {
                continue;
            } else {
                seen.add(person);
            }
            threeHop.addAll(Main.getHopThree(redisson, person, depth - 1, seen));
        }
        return threeHop;
    }
}
