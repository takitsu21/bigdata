package bigdata;

import org.json.JSONArray;
import org.json.JSONObject;
import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RListAsync;
import org.redisson.api.RMapAsync;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class InitDB {
    private final StringCodec stringCodec = new StringCodec();

    private final RedissonClient redisson;
    private final int BATCHSIZE = 30000;

    public InitDB(RedissonClient redissonClient) {
        this.redisson = redissonClient;
    }

    public static void main(String[] args) {
        Config config = null;
        try {
            config = Config.fromYAML(Main.class.getClassLoader().getResource("redis-config.yml"));
            config.setCodec(new StringCodec());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RedissonClient redisson = Redisson.create(config);

        InitDB initDB = new InitDB(redisson);

        initDB.start();
        initDB.feedback();
        initDB.brandByProduct();
        initDB.product();
        initDB.customer();
        initDB.vendor();
        initDB.order();
        initDB.person_hasInterest();
        initDB.person_knows();
        initDB.invoice();
        initDB.post();
        initDB.post_hasCreator();
        initDB.post_hasTag();
        System.exit(0);
    }

    public void start() {
        redisson.getKeys().flushdb();
        redisson.getKeys().flushall();
    }

    public void feedback() {
        String file = "./DATA/Feedback/Feedback.csv";
        String line;
        System.out.println("feedback ...");
        RBatch batch = redisson.createBatch();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RMapAsync<String, String> map = batch.getMap(row[0], stringCodec);
                map.fastPutAsync(row[1], row[2]);
            }
        } catch (Exception e) {
            System.out.println("feedback issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void brandByProduct() {
        String file = "./DATA/Product/BrandByProduct.csv";
        String line;
        System.out.println("BrandByProduct ...");
        RBatch batch = redisson.createBatch();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split(",");
                RMapAsync<String, String> map = batch.getMap(row[1], stringCodec);
                map.fastPutAsync("brand", row[0]);
            }
        } catch (Exception e) {
            System.out.println("BrandByProduct issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void product() {
        String file = "./DATA/Product/Product.csv";
        String line;
        System.out.println("Product ...");
        RBatch batch = redisson.createBatch();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            RListAsync<String> products = batch.getList("Products", stringCodec);
            while ((line = br.readLine()) != null) {
                String[] row = line.split(",");
                RMapAsync<String, String> map = batch.getMap(row[0], stringCodec);
                map.fastPutAsync("title", row[1]);
                map.fastPutAsync("price", row[2]);
                map.fastPutAsync("imgUrl", row[3]);
                products.addAsync(row[0]);
            }
        } catch (Exception e) {
            System.out.println("Product issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void customer() {
        String file = "./DATA/Customer/person_0_0.csv";
        String line;
        System.out.println("Customer ...");
        RBatch batch = redisson.createBatch();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            br.readLine();
            RListAsync<String> customerList = batch.getList("Customers", stringCodec);

            while ((line = br.readLine()) != null) {

                String[] row = line.split("\\|");
                RMapAsync<String, String> customers = batch.getMap(row[0], stringCodec);
                Date birthday = new SimpleDateFormat("yyyy-MM-dd").parse(row[4]);
                Date creationDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S").parse(row[5]);

                customers.fastPutAsync("firstName", row[1]);
                customers.fastPutAsync("lastName", row[2]);
                customers.fastPutAsync("gender", row[3]);
                customers.fastPutAsync("birthday", String.valueOf(birthday.getTime()));
                customers.fastPutAsync("creationDate", String.valueOf(creationDate.getTime()));
                customers.fastPutAsync("locationIP", row[6]);
                customers.fastPutAsync("browserUsed", row[7]);
                customers.fastPutAsync("place", row[8]);
                customerList.addAsync(row[0]);

            }
        } catch (Exception e) {
            System.out.println("Customer issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void vendor() {
        String file = "./DATA/Vendor/Vendor.csv";
        String line;
        System.out.println("Vendor ...");
        RBatch batch = redisson.createBatch();
        RListAsync<String> vendors = batch.getList("Vendors", stringCodec);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {

                String[] row = line.split(",");
                RMapAsync<String, String> map = batch.getMap(row[0], stringCodec);
                map.fastPutAsync("Country", row[1]);
                map.fastPutAsync("Industry", row[2]);
                if (!row[1].equals("England")) {
                    vendors.addAsync(row[0]);
                }
            }
        } catch (Exception e) {
            System.out.println("Vendor issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void order() {
        System.out.println("Order ...");
        RBatch batch = redisson.createBatch();
        try {
            String file = "./DATA/Order/Order.json";
            String line;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            RListAsync<String> orders = batch.getList("Orders", stringCodec);
            while ((line = bufferedReader.readLine()) != null) {
                JSONObject jsonObject = new JSONObject(line);
                String orderId = jsonObject.getString("OrderId");
                String personId = jsonObject.getString("PersonId");
                RListAsync<String> personOrders = batch.getList(
                        String.format("%s_Orders", personId), stringCodec);
                personOrders.addAsync(orderId);
                RMapAsync<String, String> ordersMap = batch.getMap(orderId, stringCodec);
                ordersMap.fastPutAsync("PersonId", personId);
                ordersMap.fastPutAsync("OrderDate", jsonObject.getString("OrderDate"));
                ordersMap.fastPutAsync("TotalPrice", String.valueOf(jsonObject.get("TotalPrice")));

                RListAsync<String> asins = batch.getList(String.format("%s_Orderline", orderId), stringCodec);
                JSONArray orderline = jsonObject.getJSONArray("Orderline");
                for (int i = 0; i < orderline.length(); i++) {
                    JSONObject orderObject = orderline.getJSONObject(i);
                    asins.addAsync(orderObject.getString("asin"));
                }
                orders.addAsync(orderId);
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void invoice() {
        System.out.println("Invoice ...");
        RBatch batch = redisson.createBatch();
        try {
            String file = "./DATA/Invoice/Invoice.xml";
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new File(file));

            RListAsync<String> orders = batch.getList("Orders", stringCodec);
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

                    RMapAsync<String, String> ordersMap = batch.getMap(orderId, stringCodec);
                    ordersMap.fastPutAsync("PersonId", personId);
                    ordersMap.fastPutAsync("OrderDate", orderDate);
                    ordersMap.fastPutAsync("TotalPrice", totalPrice);

                    RListAsync<String> asins = batch.getList(String.format("%s_Orderline", orderId), stringCodec);
                    for (int j = 0; j < nOrderLine.getLength(); j++) {
                        Node nodeJ = nOrderLine.item(j);
                        if (nodeJ.getNodeType() == Node.ELEMENT_NODE) {
                            Element eElementJ = (Element) nodeJ;
                            String asin = eElementJ.getElementsByTagName("asin").item(0).getTextContent();
                            asins.addAsync(asin);
                        }
                    }
                    orders.addAsync(orderId);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void person_hasInterest() {
        String file = "./DATA/SocialNetwork/person_hasInterest_tag_0_0.csv";
        String line;
        System.out.println("person_hasInterest ...");
        RBatch batch = redisson.createBatch();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RListAsync<String> knows = batch.getList(row[0] + "_Tags", stringCodec);
                knows.addAsync(row[1]);
            }
        } catch (Exception e) {
            System.out.println("person_hasInterest issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void person_knows() {
        String file = "./DATA/SocialNetwork/person_knows_person_0_0.csv";
        String line;
        System.out.println("person_knows ...");
        RBatch batch = redisson.createBatch();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RListAsync<String> knows = batch.getList(row[0] + "_Knows", stringCodec);
                knows.addAsync(row[1]);
            }
        } catch (Exception e) {
            System.out.println("person_knows issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void post() {
        String file = "./DATA/SocialNetwork/post_0_0.csv";
        String line;
        System.out.println("post ...");
        RBatch batch = redisson.createBatch();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            RListAsync<String> posts = batch.getList("Posts", stringCodec);

            br.readLine();
            int nbr = 0;
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RMapAsync<String, String> map = batch.getMap(row[0]);
                map.fastPutAsync("imageFile", row[1]);

                Date creationDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S").parse(row[2]);
                map.fastPutAsync("creationDate", creationDate.getTime() + "");
                map.fastPutAsync("locationIP", row[3]);
                map.fastPutAsync("browserUsed", row[4]);
                map.fastPutAsync("language", row[5]);
                map.fastPutAsync("content", row[6]);
                map.fastPutAsync("length", row[7]);
                posts.addAsync(row[0]);

                nbr++;
                if (nbr > BATCHSIZE) {
                    batch.execute();
                    batch = redisson.createBatch();
                    nbr = 0;
                }
            }
        } catch (Exception e) {
            System.out.println("post issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void post_hasCreator() {
        String file = "./DATA/SocialNetwork/post_hasCreator_person_0_0.csv";
        String line;
        System.out.println("post_hasCreator ...");
        RBatch batch = redisson.createBatch();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RListAsync<String> knows = batch.getList(row[1] + "_Posts", stringCodec);
                knows.addAsync(row[0]);

                RMapAsync<String, String> map = batch.getMap(row[0]);
                map.fastPutAsync("creator", row[1]);
            }
        } catch (Exception e) {
            System.out.println("post_hasCreator issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }

    public void post_hasTag() {
        String file = "./DATA/SocialNetwork/post_hasTag_tag_0_0.csv";
        String line;
        System.out.println("post_hasTag ...");
        RBatch batch = redisson.createBatch();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RListAsync<String> knows = batch.getList(row[0] + "_Tags", stringCodec);
                knows.addAsync(row[1]);
            }
        } catch (Exception e) {
            System.out.println("post_hasTag  issues");
            e.printStackTrace();
        }
        batch.execute();
        System.out.println("✔");
    }
}
