package bigdata;

import org.json.JSONArray;
import org.json.JSONObject;
import org.redisson.Redisson;
import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class InitDB {
    private final StringCodec stringCodec = new StringCodec();

    private final RedissonClient redisson;

    public InitDB(RedissonClient redissonClient) {
        this.redisson = redissonClient;
    }


    public static void main(String[] args) {
        Config config = null;
        try {
            config = Config.fromYAML(Main.class.getClassLoader().getResource("redis-config.yml"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RedissonClient redisson = Redisson.create(config);

        InitDB initDB = new InitDB(redisson);

//        initDB.start();
//        initDB.feedback();
//        initDB.brandByProduct();
//        initDB.product();
//        initDB.customer();
//        initDB.vendor();
//        initDB.order();
//        initDB.person_hasInterest();
//        initDB.person_knows();
//        initDB.invoice();
//        initDB.post();
//        initDB.post_hasCreator();
//        initDB.post_hasTag();
    }


    public void start() {
        redisson.getKeys().flushdb();
        redisson.getKeys().flushall();
    }

    public void feedback() {
        String file = "../DATA/feedback/feedback.csv";
        String line;
        System.out.println("feedback ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RMap<String, String> map = redisson.getMap(row[0], stringCodec);
                map.fastPut(row[1], row[2]);
            }
        } catch (Exception e) {
            System.out.println("feedback issues");
            e.printStackTrace();
        }

        System.out.println("done feedback");
    }

    public void brandByProduct() {
        String file = "../DATA/Product/BrandByProduct.csv";
        String line;
        System.out.println("BrandByProduct ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split(",");
                RMap<String, String> map = redisson.getMap(row[1], stringCodec);
                map.fastPut("brand", row[0]);
            }
        } catch (Exception e) {
            System.out.println("BrandByProduct issues");
            e.printStackTrace();
        }

        System.out.println("done BrandByProduct");
    }

    public void product() {
        String file = "../DATA/Product/Product.csv";
        String line;
        System.out.println("Product ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split(",");
                RMap<String, String> map = redisson.getMap(row[0], stringCodec);
                map.fastPut("title", row[1]);
                map.fastPut("price", row[2]);
                map.fastPut("imgUrl", row[3]);
            }
        } catch (Exception e) {
            System.out.println("Product issues");
            e.printStackTrace();
        }

        System.out.println("done Product");
    }

    public void customer() {
        String file = "../DATA/Customer/person_0_0.csv";
        String line;
        System.out.println("Customer ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            RList<String> customerList = redisson.getList("Customers", stringCodec);

            while ((line = br.readLine()) != null) {

                String[] row = line.split("\\|");
                RMap<String, Object> customers = redisson.getMap(row[0], stringCodec);
                Date birthday = new SimpleDateFormat("yyyy-MM-dd").parse(row[4]);
                Date creationDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S").parse(row[5]);

                customers.fastPut("firstName", row[1]);
                customers.fastPut("lastName", row[2]);
                customers.fastPut("gender", row[3]);
                customers.fastPut("birthday", Long.parseLong(String.valueOf(birthday.getTime())));
                customers.fastPut("creationDate", Long.parseLong(String.valueOf(creationDate.getTime())));
                customers.fastPut("locationIP", row[6]);
                customers.fastPut("browserUsed", row[7]);
                customers.fastPut("place", Float.parseFloat(row[8]));
                customerList.add(row[0]);


            }
        } catch (Exception e) {
            System.out.println("Customer issues");
            e.printStackTrace();
        }
        System.out.println("done Customer");
    }

    public void vendor() {
        String file = "../DATA/Vendor/Vendor.csv";
        String line;
        System.out.println("Vendor ...");
        RList<String> vendors = redisson.getList("Vendors", stringCodec);
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {

                String[] row = line.split(",");
                RMap<String, String> map = redisson.getMap(row[0], stringCodec);
                map.fastPut("Country", row[1]);
                map.fastPut("Industry", row[2]);
                if (!row[1].equals("England")) {
                    vendors.add(row[0]);
                }
            }
        } catch (Exception e) {
            System.out.println("Vendor issues");
            e.printStackTrace();
        }
        System.out.println("done Vendor");
    }

    public void order() {

        System.out.println("Order ...");
        try {
            String file = "../DATA/Order/Order.json";
            String line;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            RList<String> orders = redisson.getList("Orders", stringCodec);
            while ((line = bufferedReader.readLine()) != null) {
                JSONObject jsonObject = new JSONObject(line);
                String orderId = jsonObject.getString("OrderId");
                String personId = jsonObject.getString("PersonId");
                RList<String> personOrders = redisson.getList(
                        String.format("%s_Orders", personId), stringCodec);
                personOrders.add(orderId);
                RMap<String, String> ordersMap = redisson.getMap(orderId, stringCodec);
                ordersMap.fastPut("PersonId", personId);
                ordersMap.fastPut("OrderDate", jsonObject.getString("OrderDate"));
                ordersMap.fastPut("TotalPrice", String.valueOf(jsonObject.get("TotalPrice")));


                RList<String> asins = redisson.getList(String.format("%s_Orderline", orderId), stringCodec);
                JSONArray orderline = jsonObject.getJSONArray("Orderline");
                for (int i = 0; i < orderline.length(); i++) {
                    JSONObject orderObject = orderline.getJSONObject(i);
                    asins.add(orderObject.getString("asin"));
                }
                orders.add(orderId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("done Order");
    }

    public void invoice() {

        System.out.println("Invoice ...");
        try {
            String file = "../DATA/Invoice/Invoice.xml";
            String line;
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new File(file));

            RList<String> orders = redisson.getList("Orders", stringCodec);
            NodeList nList = document.getElementsByTagName("Invoice.xml");
            for (int i = 0; i < nList.getLength(); i++) {
                Node node = nList.item(i);

                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) node;
                    NodeList nOrderLine = eElement.getElementsByTagName("Orderline");
                    String orderId = eElement.getElementsByTagName("OrderId").item(0).getTextContent();
                    String personId = eElement.getElementsByTagName("PersonId").item(0).getTextContent();
                    String orderDate = eElement.getElementsByTagName("OrderDate").item(0).getTextContent();
                    String totalPrice = eElement.getElementsByTagName("TotalPrice").item(0).getTextContent();

                    RMap<String, String> ordersMap = redisson.getMap(orderId, stringCodec);
                    ordersMap.fastPut("PersonId", personId);
                    ordersMap.fastPut("OrderDate", orderDate);
                    ordersMap.fastPut("TotalPrice", totalPrice);


                    RList<String> asins = redisson.getList(String.format("%s_Orderline", orderId), stringCodec);
                    for (int j = 0; j < nOrderLine.getLength(); j++) {
                        Node nodeJ = nOrderLine.item(j);
                        if (nodeJ.getNodeType() == Node.ELEMENT_NODE) {
                            Element eElementJ = (Element) nodeJ;
                            String asin = eElementJ.getElementsByTagName("asin").item(0).getTextContent();
                            asins.add(asin);
                        }
                    }
                    orders.add(orderId);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("done Invoice");
    }


    public void person_hasInterest() {
        String file = "../DATA/SocialNetwork/person_hasInterest_tag_0_0.csv";
        String line;
        System.out.println("person_hasInterest ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RList<String> knows = redisson.getList(row[0] + "_Tags", stringCodec);
                knows.add(row[1]);
            }
        } catch (Exception e) {
            System.out.println("person_hasInterest issues");
            e.printStackTrace();
        }
        System.out.println("done person_hasInterest");
    }

    public void person_knows() {
        String file = "../DATA/SocialNetwork/person_knows_person_0_0.csv";
        String line;
        System.out.println("person_knows ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RList<String> knows = redisson.getList(row[0] + "_Knows", stringCodec);
                knows.add(row[1]);
            }
        } catch (Exception e) {
            System.out.println("person_knows issues");
            e.printStackTrace();
        }
        System.out.println("doneperson_knows");
    }

    public void post() {
        String file = "../DATA/SocialNetwork/post_0_0.csv";
        String line;
        System.out.println("post ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            RList<String> posts = redisson.getList("Posts");
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RMap<String, String> map = redisson.getMap(row[0], stringCodec);
                map.fastPut("imageFile", row[1]);

                Date creationDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S").parse(row[2]);
                map.fastPut("creationDate", creationDate.getTime() + "");
                map.fastPut("locationIP", row[3]);
                map.fastPut("browserUsed", row[4]);
                map.fastPut("language", row[5]);
                map.fastPut("content", row[6]);
                map.fastPut("length", row[7]);
                posts.add(row[0]);
            }
        } catch (Exception e) {
            System.out.println("post issues");
            e.printStackTrace();
        }
        System.out.println("done post");
    }

    public void post_hasCreator() {
        String file = "../DATA/SocialNetwork/post_hasCreator_person_0_0.csv";
        String line;
        System.out.println("post_hasCreator ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RList<String> knows = redisson.getList(row[1] + "_Posts", stringCodec);
                knows.add(row[0]);

                RMap<String, String> map = redisson.getMap(row[0]);
                map.fastPut("creator", row[1]);
            }
        } catch (Exception e) {
            System.out.println("post_hasCreator issues");
            e.printStackTrace();
        }
        System.out.println("done post_hasCreator");
    }

    public void post_hasTag() {
        String file = "../DATA/SocialNetwork/post_hasTag_tag_0_0.csv";
        String line;
        System.out.println("post_hasTag ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RList<String> knows = redisson.getList(row[0] + "_Tags", stringCodec);
                knows.add(row[1]);
            }
        } catch (Exception e) {
            System.out.println("post_hasTag  issues");
            e.printStackTrace();
        }
        System.out.println("done post_hasTag ");
    }
}
