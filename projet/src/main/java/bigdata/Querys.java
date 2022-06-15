package bigdata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.DoubleCodec;
import org.redisson.client.codec.StringCodec;

import bigdata.Utils.Company;
import bigdata.Utils.RFM;

public class Querys {
    public final RedissonClient redisson;
    public final StringCodec codecString = new StringCodec();
    public final DoubleCodec codecDouble = new DoubleCodec();

    public Querys(RedissonClient redisson) {
        this.redisson = redisson;
    }

    /**
     * Query 1. For a given customer, find his/her all related data including profile,
     * orders, invoices, feedback, comments, and posts in the last month, return the
     * category in which he/she has bought the largest number of products, and return
     * the tag which he/she has engaged the greatest times in the posts.
     */
    public List<String> Query1(String customerId) {
        RList<String> orders = redisson.getList(String.format("%s_Orders", customerId),
                codecString);
        Map<String, Integer> productBought = new HashMap<>();
        Map<String, Integer> tagUsed = new HashMap<>();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        Date result = cal.getTime();

        for (String orderId : orders) {
            RList<String> products = redisson.getList(String.format("%s_Orderline", orderId), codecString);
            for (String productId : products) {
                RMap<String, String> product = redisson.getMap(productId, codecString);
                String brand = product.get("brand");
                if (!productBought.containsKey(brand)) {
                    productBought.put(brand, 1);
                } else {
                    productBought.put(brand, productBought.get(brand) + 1);
                }
            }
        }

        RList<String> posts = redisson.getList(customerId + "_Posts", codecString);

        for (String postId : posts) {
            RList<String> tags = redisson.getList(postId + "_Tags", codecString);
            for (String tag : tags) {
                if (!tagUsed.containsKey(tag)) {
                    tagUsed.put(tag, 1);
                } else {
                    tagUsed.put(tag, tagUsed.get(tag) + 1);
                }
            }
        }
        String mostTagUsed = (String) Utils.getMaxFromMap(tagUsed);
        String mostPopularIndustry = (String) Utils.getMaxFromMap(productBought);

        System.out.println(mostPopularIndustry);
        System.out.println(mostTagUsed);

        return Collections.emptyList();
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
                        if (feedbackWithNote.startsWith("'5.", 1) || feedbackWithNote.startsWith("5.", 1)) {
                            String feedback = feedbackWithNote.substring(5);
                            allFeedback.add(feedback);
                        }


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
                .limit(3).map(Map.Entry::getKey).collect(Collectors.toList());
    }

    /**
     * Query 7. For the products of a given vendor with declining sales compare to the former
     * quarter, analyze the reviews for these items to see if there are any negative sentiments.
     *
     * @param vendor
     * @return
     */
    List<String> Query7(String vendor) {
        RList<String> products = redisson.getList("Products", codecString);
        List<String> query = new ArrayList<>();
        for (String productId : products) {
            RMap<String, String> product = redisson.getMap(productId, codecString);
            List<String> clientsWithFeedBack = product.readAllKeySet().stream()
                    .filter(s -> !Set.of("brand", "title", "price", "imgUrl").contains(s)).toList();
            if (product.get("brand") != null && product.get("brand").equals(vendor)) {
                for (String PersonId : clientsWithFeedBack) {
                    String[] splitReview = product.get(PersonId).split(",");
                    float rate = Float.parseFloat(splitReview[0].replace("'", ""));
                    String review = String.join("", Arrays.copyOfRange(splitReview, 1, splitReview.length));
                    if (product.get("brand") != null
                            && product.get("brand").equals(vendor)
                            && rate < 2.5) {
                        query.add(review);
                    }
                }
            }
        }
        return query;
    }

    /**
     * Query 8. For all the products of a given category during a given year,
     * compute its total sales amount, and measure its popularity in the social media.
     */
    public void Query8(String categorie, String date) throws ParseException {
        double totalSales = 0;
        Set<String> allNames = new HashSet<>();

        Date startDate = new SimpleDateFormat("dd/MM/yyyy").parse(date);
        Calendar c = Calendar.getInstance();
        c.setTime(startDate);
        c.add(Calendar.YEAR, 1);
        Date endDate = c.getTime();

        RList<String> orders = redisson.getList("Orders", codecString);
        for (String order : orders) {

            Date dateOrder = new SimpleDateFormat("yyyy-MM-dd").parse((String) redisson.getMap(order, codecString).get("OrderDate"));
            double price = (double) redisson.getMap(order, codecDouble).get("TotalPrice");

            RList<String> products = redisson.getList(order + "_Orderline", codecString);
            for (String product : products) {
                RMap<String, String> map = redisson.getMap(product, codecString);

                String brand = map.get("brand");
                if (dateOrder.before(endDate) && dateOrder.after(startDate) && brand.equals(categorie)) {
                    totalSales += price;
                    if (map.get("title") != null) allNames.add(map.get("title"));
                }
            }

        }
        RList<String> posts = redisson.getList("Posts", codecString);
        int nbrPostDuringYear = 0;
        int nbrPostWithThisCategorie = 0;
        for (String postId : posts) {
            if (postId.startsWith(">", 1)) {
                String feedback = postId.substring(3);
                nbrPostDuringYear += 1;
                String content = (String) redisson.getMap(feedback, codecString).get("content");
                for (String name : allNames) {
                    if (content.toUpperCase().contains(name.toUpperCase())) {
                        nbrPostWithThisCategorie += 1;
                        break;
                    }
                }


            }

        }
        System.out.println("% de popularité : " + (nbrPostWithThisCategorie / nbrPostDuringYear) * 100);


        System.out.println("total sales : " + totalSales);

    }

    /**
     * Query 9. Find top-3 companies who have the largest amount of sales at one
     * country, for each
     * company, compare the number of the male and female customers, and return the
     * most recent
     * posts of them.
     */
    public Map<String, Company> Query9(String country) {
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
                    company.customers.add(personId);
                    if (gender.equals("male")) {
                        company.males++;
                    } else {
                        company.females++;
                    }
                }
            }

        }

        Map<String, Company> bestsCompanies = sales.entrySet().stream()
                .sorted(Map.Entry.comparingByValue((c1, c2) -> Integer.compare(c2.sales, c1.sales)))
                .limit(3).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        for (Map.Entry<String, Company> company : bestsCompanies.entrySet()) {
            for (String personId : company.getValue().customers) {
                RList<String> posts = redisson.getList(personId + "_Posts", codecString);
                Double lastTime = 0.;
                String lastPost = "";

                for (String postId : posts) {
                    Double creationDate = (Double) redisson.getMap(postId, codecDouble).get("creationDate");
                    if (creationDate > lastTime) {
                        lastTime = creationDate;
                        lastPost = postId;
                    }
                }
                company.getValue().posts.add(lastPost);
            }
        }

        return bestsCompanies;
    }

    public Map<String, RFM> Query10() {
        Map<String, RFM> persons = new HashMap<>();
        long lastYear = LocalDate.now().minusYears(1).toEpochDay() * 24 * 60 * 60;
        long lastMonth = LocalDate.now().minusMonths(1).toEpochDay() * 24 * 60 * 60;
        long lastWeek = LocalDate.now().minusWeeks(1).toEpochDay() * 24 * 60 * 60;
        RList<String> posts = redisson.getList("Posts", codecString);
        for (String postId : posts) {
            RMap<String, String> map = redisson.getMap(postId, codecString);
            double date = Double.parseDouble(map.get("creationDate"));
            if (date > lastYear) {
                String creator = map.get("creator");
                RList<String> tags = redisson.getList(postId + "_Tags", codecString);
                RFM rfm = persons.get(creator);
                if (rfm == null) {
                    persons.put(creator, new RFM());
                }
                rfm.tags.addAll(tags);
                rfm.frequency++;
                if (date > lastWeek) {
                    rfm.recency += 5;
                } else if (date > lastMonth) {
                    rfm.recency += 3;
                } else {
                    rfm.recency++;
                }
            }
        }

        return persons.entrySet().stream()
                .sorted(Map.Entry.comparingByValue((c1, c2) -> Integer.compare(c2.frequency, c1.frequency)))
                .limit(10).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


}
