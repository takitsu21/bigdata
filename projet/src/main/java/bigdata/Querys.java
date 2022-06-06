package bigdata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.DoubleCodec;
import org.redisson.client.codec.StringCodec;

public class Querys {
    public final RedissonClient redisson;
    public final StringCodec codecString = new StringCodec();
    public final DoubleCodec codecDouble = new DoubleCodec();

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

        RMap<String, String> map = redisson.getMap(ProductId, codecString);

        List<String> clientsWithFeedBack = map.readAllKeySet().stream()
                .filter(s -> !Set.of("brand", "title", "price", "imgUrl").contains(s)).toList();

        for (String PersonId : clientsWithFeedBack) {
            RList<String> posts = redisson.getList(PersonId + "_Posts", codecString);

            for (String postID : posts) {
                RMap<String, String> post = redisson.getMap(postID, codecString);

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

        RMap<String, String> map = redisson.getMap(ProductId, codecString);

        List<String> clientsWithFeedBack = map.readAllKeySet().stream()
                .filter(s -> !Set.of("brand", "title", "price", "imgUrl").contains(s)).toList();

        for (String PersonId : clientsWithFeedBack) {
            RList<String> posts = redisson.getList(PersonId + "_Posts", codecString);
            String[] splitReview = map.get(PersonId).split(",");
            float rate = Float.parseFloat(splitReview[0].replace("'", ""));
            String review = String.join("", Arrays.copyOfRange(splitReview, 1, splitReview.length));

            for (String postID : posts) {
                RMap<String, String> post = redisson.getMap(postID, codecString);

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

        RList<String> persons = redisson.getList("Customers", codecString);
        for (String personID : persons) {
            RList<String> ordersID = redisson.getList(personID + "_Orders", codecString);
            double sum = 0;
            for (String orderID : ordersID) {
                Double totalPrice = (Double) redisson.getMap(orderID, codecDouble).get("TotalPrice");
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

        Set<String> hopThreeP1 = Utils.getHopThree(redisson, person1, 3, new HashSet<>());
        Set<String> hopThreeP2 = Utils.getHopThree(redisson, person2, 3, new HashSet<>());

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
        Set<String> friendsCustomer = Utils.getHopThree(redisson, customer, 3, new HashSet<>());
        Set<String> allFeedback = new HashSet<>();
        for (String personId : friendsCustomer) {

            RList<String> orders = redisson.getList(personId + "_Orders", codecString);
            Set<String> ordersAsin = new HashSet<>();
            orders.forEach(order -> ordersAsin.addAll(redisson.getList(order + "_Orderline", codecString)));

            for (String orderAsin : ordersAsin) {
                if (redisson.getMap(orderAsin, codecString).get("brand").equals(productCategorie)) {
                    String feedbackWithNote = (String) redisson.getMap(orderAsin, codecString).get(personId);
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
        List<String> path = Utils.getHopThreePath(redisson, customer1, customer2);
        Map<String, Double> sellers = new HashMap<>();
        for (String person : path) {
            RList<String> orders = redisson.getList(person + "_Orders", codecString);
            for (String order : orders) {
                RList<String> products = redisson.getList(order + "_Orderline", codecString);
                for (String product : products) {
                    Double nbr = sellers.get(product);
                    sellers.put(product, nbr == null ? 1 : nbr + 1);
                }
            }
        }
        return sellers.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(3).map(entry -> entry.getKey()).collect(Collectors.toList());
    }

    /**
     * Query 9. Find top-3 companies who have the largest amount of sales at one
     * country, for each
     * company, compare the number of the male and female customers, and return the
     * most recent
     * posts of them.
     */
    public List<String> Query9(String country) {
        class Company {
            int sales = 0;
            int males = 0;
            int females = 0;

            @Override
            public String toString() {
                return "sales: " + sales + "\nmales: " + males + "\nfemales: " + females;
            }
        }

        RList<String> vendors = redisson.getList("Vendors", codecString);
        Map<String, Company> sales = vendors.stream()
                .filter(vendor -> redisson.getMap(vendor, codecString).get("Country").equals(country))
                .collect(Collectors.toMap(vendor -> vendor, vendor -> new Company()));

        RList<String> orders = redisson.getList("Orders", codecString);
        for (String orderId : orders) {
            RMap<String, String> order = redisson.getMap(orderId, codecString);
            String personId = order.get("PersonId");
            RMap<String, String> person = redisson.getMap(personId, codecString);
            String gender = person.get("gender");

            RList<String> products = redisson.getList(orderId + "_Orderline", codecString);
            for (String product : products) {
                RMap<String, String> map = redisson.getMap(product, codecString);
                String brand = map.get("brand");
                Company company = sales.get(brand);
                if (company != null) {
                    company.sales++;
                    if (gender.equals("male")) {
                        company.males++;
                    } else {
                        company.females++;
                    }
                }
            }

        }

        System.out.println(sales);

        List<String> bestsCompanies = sales.entrySet().stream()
                .sorted(Map.Entry.comparingByValue((c1, c2) -> Integer.compare(c2.sales, c1.sales)))
                .limit(3).map(entry -> entry.getKey()).collect(Collectors.toList());

        System.out.println(bestsCompanies);

        return new ArrayList<>();
    }

}
