import bigdata.Main;
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

public class init {


    public static void main(String[] args) {
        Config config = null;
        try {
            config = Config.fromYAML(Main.class.getClassLoader().getResource("redis-config.yml"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RedissonClient redisson = Redisson.create(config);

        start(redisson);

        //feedback(redisson);

        //brandByProduct(redisson);

        //product(redisson);

        //customer(redisson);

        //vendor(redisson);

        //order(redisson);


        //person_hasInterest(redisson);

        //person_hasInterest(redisson);
        order(redisson);

//        invoice(redisson);
        //post(redisson);

        //post_hasCreator(redisson);

        //post_hasTag(redisson);
    }


    public static void start(RedissonClient redisson) {
        redisson.getKeys().flushdb();
        redisson.getKeys().flushall();
    }

    public static void feedback(RedissonClient redisson) {
        String file = "DATA/feedback/feedback.csv";
        String line;
        System.out.println("feedback ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RMap<String, String> map = redisson.getMap(row[0]);
                map.put(row[1], row[2]);
            }
        } catch (Exception e) {
            System.out.println("feedback issues");
            System.out.println(e);
        }

        System.out.println("done feedback");
    }

    public static void brandByProduct(RedissonClient redisson) {
        String file = "DATA/Product/BrandByProduct.csv";
        String line;
        System.out.println("BrandByProduct ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split(",");
                RMap<String, String> map = redisson.getMap(row[1]);
                map.put("brand", row[0]);
            }
        } catch (Exception e) {
            System.out.println("BrandByProduct issues");
            System.out.println(e);
        }

        System.out.println("done BrandByProduct");
    }

    public static void product(RedissonClient redisson) {
        String file = "DATA/Product/Product.csv";
        String line;
        System.out.println("Product ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split(",");
                RMap<String, String> map = redisson.getMap(row[0]);
                map.put("title", row[1]);
                map.put("price", row[2]);
                map.put("imgUrl", row[3]);
            }
        } catch (Exception e) {
            System.out.println("Product issues");
            System.out.println(e);
        }

        System.out.println("done Product");
    }

    public static void customer(RedissonClient redisson) {
        String file = "DATA/Customer/person_0_0.csv";
        String line;
        System.out.println("Customer ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {

                String[] row = line.split("\\|");
                RList<String> list = redisson.getList(row[0]);

                list.add(row[1]);
                list.add(row[2]);
                list.add(row[3]);
                Date birthday = new SimpleDateFormat("yyyy-MM-dd").parse(row[4]);
                list.add(birthday.getTime() + "");

                Date creationDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S").parse(row[5]);

                list.add(creationDate.getTime() + "");

                list.add(row[6]);
                list.add(row[7]);
                list.add(row[8]);

            }
        } catch (Exception e) {
            System.out.println("Customer issues");
            System.out.println(e);
        }
        System.out.println("done Customer");
    }

    public static void vendor(RedissonClient redisson) {
        String file = "DATA/Vendor/Vendor.csv";
        String line;
        System.out.println("Vendor ...");
        RList<String> vendors = redisson.getList("Vendors");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {

                String[] row = line.split(",");
                RMap<String, String> map = redisson.getMap(row[0]);
                map.put("Country", row[1]);
                map.put("Industry", row[2]);
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

    public static void order(RedissonClient redisson) {

        System.out.println("Order ...");
        try {
            String file = "../DATA/Order/Order.json";
            String line;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            RList<String> orders = redisson.getList("Orders", new StringCodec());
            while ((line = bufferedReader.readLine()) != null) {
                JSONObject jsonObject = new JSONObject(line);
                String orderId = (String) jsonObject.get("OrderId");
                String personId = (String) jsonObject.get("PersonId");
                RMap<String, String> personOrders = redisson.getMap(
                        String.format("%s_Orders", personId), new StringCodec());
                personOrders.fastPut(String.format("%s_Orders", personId), orderId);
                RMap<String, String> ordersMap = redisson.getMap(orderId, new StringCodec());
                ordersMap.fastPut("PersonId", personId);
                ordersMap.fastPut("OrderDate", (String) jsonObject.get("OrderDate"));
                ordersMap.fastPut("TotalPrice", String.valueOf(jsonObject.get("TotalPrice")));


                RList<String> asins = redisson.getList(String.format("%s_Orderline", orderId), new StringCodec());
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

    public static void invoice(RedissonClient redisson) {

        System.out.println("Invoice ...");
        try {
            String file = "../DATA/Invoice/Invoice.xml";
            String line;
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new File(file));
            Element root = document.getDocumentElement();

            RList<String> orders = redisson.getList("Orders", new StringCodec());
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

                    RMap<String, String> ordersMap = redisson.getMap(orderId, new StringCodec());
                    ordersMap.fastPut("PersonId", personId);
                    ordersMap.fastPut("OrderDate", orderDate);
                    ordersMap.fastPut("TotalPrice", totalPrice);


                    RList<String> asins = redisson.getList(String.format("%s_Orderline", orderId), new StringCodec());
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


    public static void person_hasInterest(RedissonClient redisson) {
        String file = "DATA/SocialNetwork/person_hasInterest_tag_0_0.csv";
        String line;
        System.out.println("person_hasInterest ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RList<String> knows = redisson.getList(row[0] + "_Tags");
                knows.add(row[1]);
            }
        } catch (Exception e) {
            System.out.println("person_hasInterest issues");
            System.out.println(e);
        }
        System.out.println("done person_hasInterest");
    }

    public static void person_knows(RedissonClient redisson) {
        String file = "DATA/SocialNetwork/person_knows_person_0_0.csv";
        String line;
        System.out.println("person_knows ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RList<String> knows = redisson.getList(row[0] + "_Knows");
                knows.add(row[1]);
            }
        } catch (Exception e) {
            System.out.println("person_knows issues");
            System.out.println(e);
        }
        System.out.println("doneperson_knows");
    }

    public static void post(RedissonClient redisson) {
        String file = "DATA/SocialNetwork/post_0_0.csv";
        String line;
        System.out.println("post ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            RList<String> posts = redisson.getList("Posts");
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RMap<String, String> map = redisson.getMap(row[0]);
                map.put("imageFile", row[1]);

                Date creationDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S").parse(row[2]);
                map.put("creationDate", creationDate.getTime() + "");
                map.put("locationIP", row[3]);
                map.put("browserUsed", row[4]);
                map.put("language", row[5]);
                map.put("content", row[6]);
                map.put("length", row[7]);
                posts.add(row[0]);
            }
        } catch (Exception e) {
            System.out.println("post issues");
            System.out.println(e);
        }
        System.out.println("done post");
    }

    public static void post_hasCreator(RedissonClient redisson) {
        String file = "DATA/SocialNetwork/post_hasCreator_person_0_0.csv";
        String line;
        System.out.println("post_hasCreator ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RList<String> knows = redisson.getList(row[1] + "_Posts");
                knows.add(row[0]);

                RMap<String, String> map = redisson.getMap(row[0]);
                map.put("creator", row[1]);
            }
        } catch (Exception e) {
            System.out.println("post_hasCreator issues");
            System.out.println(e);
        }
        System.out.println("done post_hasCreator");
    }

    public static void post_hasTag(RedissonClient redisson) {
        String file = "DATA/SocialNetwork/post_hasTag_tag_0_0.csv";
        String line;
        System.out.println("post_hasTag ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] row = line.split("\\|");
                RList<String> knows = redisson.getList(row[0] + "_Tags");
                knows.add(row[1]);
            }
        } catch (Exception e) {
            System.out.println("post_hasTag  issues");
            System.out.println(e);
        }
        System.out.println("done post_hasTag ");
    }
}
