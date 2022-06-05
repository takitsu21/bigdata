package bigdata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.DoubleCodec;
import org.redisson.client.codec.StringCodec;

public class Querys {
    public final RedissonClient redisson;

    public Querys(RedissonClient redisson) {
        this.redisson = redisson;
    }

    /**
     * Query 2:
     * For a given product during a given period, find the people who commented or
     * posted on it, and had bought it.
     * 
     * @return
     * @throws ParseException
     */
    public List<String> Query2(String ProductId, String before, String after)
            throws ParseException {
        List<String> query = new ArrayList<>();
        Date beforeDate = new SimpleDateFormat("dd/MM/yyyy").parse(before);
        Date afterDate = new SimpleDateFormat("dd/MM/yyyy").parse(after);

        StringCodec codec = new StringCodec();
        RMap<String, String> map = this.redisson.getMap(ProductId, codec);

        List<String> clientsWithFeedBack = map.readAllKeySet().stream()
                .filter(s -> !Set.of("brand", "title", "price", "imgUrl").contains(s)).toList();

        for (String PersonId : clientsWithFeedBack) {
            RList<String> posts = this.redisson.getList(PersonId + "_Posts", codec);

            for (String postID : posts) {
                RMap<String, String> post = this.redisson.getMap(postID, codec);

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
     * Query 3. For a given product during a given period, find people who have
     * undertaken
     * activities related to it, e.g., posts, comments, and review, and return
     * sentences from these texts
     * that contain negative sentiments
     * 
     * @param ProductId
     * @param before
     * @param after
     * @return
     * @throws ParseException
     */
    public List<String> Query3(String ProductId, String before, String after)
            throws ParseException {
        List<String> query = new ArrayList<>();
        Date beforeDate = new SimpleDateFormat("dd/MM/yyyy").parse(before);
        Date afterDate = new SimpleDateFormat("dd/MM/yyyy").parse(after);

        StringCodec codec = new StringCodec();
        RMap<String, String> map = this.redisson.getMap(ProductId, codec);

        List<String> clientsWithFeedBack = map.readAllKeySet().stream()
                .filter(s -> !Set.of("brand", "title", "price", "imgUrl").contains(s)).toList();

        for (String PersonId : clientsWithFeedBack) {
            RList<String> posts = this.redisson.getList(PersonId + "_Posts", codec);
            String[] splitReview = map.get(PersonId).split(",");
            float rate = Float.parseFloat(splitReview[0].replace("'", ""));
            String review = String.join("", Arrays.copyOfRange(splitReview, 1, splitReview.length));

            for (String postID : posts) {
                RMap<String, String> post = this.redisson.getMap(postID, codec);

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
     * @return
     * @throws ParseException
     */
    public List<String> Query4() {
        String person1 = "";
        String person2 = "";
        double pricePerson1 = 0;
        double pricePerson2 = 0;

        StringCodec codecString = new StringCodec();
        DoubleCodec codecPrice = new DoubleCodec();

        RList<String> persons = this.redisson.getList("Customers", codecString);
        for (String personID : persons) {
            RList<String> ordersID = this.redisson.getList(personID + "_Orders", codecString);
            double sum = 0;
            for (String orderID : ordersID) {
                Double totalPrice = (Double) this.redisson.getMap(orderID, codecPrice).get("TotalPrice");
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

            ;
        }

        Set<String> hopThreeP1 = Utils.getHopThree(this.redisson, person1, 3, new HashSet<>());
        Set<String> hopThreeP2 = Utils.getHopThree(this.redisson, person2, 3, new HashSet<>());

        return hopThreeP1.stream()
                .distinct()
                .filter(hopThreeP2::contains)
                .collect(Collectors.toList());
    }

    /**
     * Query 5. Given a start customer and a product category,
     * find persons who are this customer's friends within 3-hop friendships in
     * Knows graph,
     * besides, they have bought products in the given category.
     * Finally, return feedback with the 5-rating review of those bought products.
     *
     * @return
     * @throws ParseException
     */
    public Set<String> Query5(String customer, String productCategorie) {
        StringCodec codec = new StringCodec();
        Set<String> friendsCustomer = Utils.getHopThree(this.redisson, customer, 3, new HashSet<>());
        Set<String> allFeedback = new HashSet<>();
        for (String personId : friendsCustomer) {

            RList<String> orders = this.redisson.getList(personId + "_Orders", codec);
            Set<String> ordersAsin = new HashSet<>();
            orders.forEach(order -> ordersAsin.addAll(this.redisson.getList(order + "_Orderline", codec)));

            for (String orderAsin : ordersAsin) {
                if (this.redisson.getMap(orderAsin, codec).get("brand").equals(productCategorie)) {
                    String feedbackWithNote = (String) this.redisson.getMap(orderAsin, codec).get(personId);
                    if (feedbackWithNote != null) {
                        double note = Double.parseDouble(feedbackWithNote.substring(1, 4));
                        String feedback = feedbackWithNote.substring(5);
                        if (note == 5.)
                            allFeedback.add(feedback);
                    }
                }
            }
        }
        return allFeedback;
    }

    /**
     * Query 6. Given customer 1 and customer 2, find persons in the shortest path
     * between them
     * in the subgraph, and return the TOP 3 best sellers from all these persons'
     * purchases.
     * 
     * @param customer1
     * @param customer2
     * @return
     */
    public List<String> Query6(String customer1, String customer2) {

        return new ArrayList<>();

    }

}
