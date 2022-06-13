import bigdata.Main;
import org.redisson.Redisson;
import org.redisson.RedissonMap;
import org.redisson.api.RBucket;
import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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

        order(redisson);

    }


    public static void start(RedissonClient redisson){
        redisson.getKeys().flushdb();
        redisson.getKeys().flushall();
    }

    public static void feedback(RedissonClient redisson){
        String file = "DATA/feedback/feedback.csv";
        String line;
        System.out.println("feedback ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            while((line = br.readLine()) != null){
                String[] row = line.split("\\|");
                RMap<String, String> map = redisson.getMap(row[0]);
                map.put(row[1],row[2]);
            }
        } catch (Exception e){
            System.out.println("feedback issues");
            System.out.println(e);
        }

        System.out.println("done feedback");
    }

    public static void brandByProduct(RedissonClient redisson){
        String file = "DATA/Product/BrandByProduct.csv";
        String line;
        System.out.println("BrandByProduct ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            while((line = br.readLine()) != null){
                String[] row = line.split(",");
                RMap<String, String> map = redisson.getMap(row[1]);
                map.put("brand",row[0]);
            }
        } catch (Exception e){
            System.out.println("BrandByProduct issues");
            System.out.println(e);
        }

        System.out.println("done BrandByProduct");
    }

    public static void product(RedissonClient redisson){
        String file = "DATA/Product/Product.csv";
        String line;
        System.out.println("Product ...");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            while((line = br.readLine()) != null){
                String[] row = line.split(",");
                RMap<String, String> map = redisson.getMap(row[0]);
                map.put("title",row[1]);
                map.put("price",row[2]);
                map.put("imgUrl",row[3]);
            }
        } catch (Exception e){
            System.out.println("Product issues");
            System.out.println(e);
        }

        System.out.println("done Product");
    }

    public static void customer(RedissonClient redisson){
        String file = "DATA/Customer/person_0_0.csv";
        String line;
        System.out.println("Customer ...");

        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while((line = br.readLine()) != null){

                String[] row = line.split("\\|");
                RList<String> list = redisson.getList(row[0]);

                list.add(row[1]);
                list.add(row[2]);
                list.add(row[3]);
                Date birthday = new SimpleDateFormat("yyyy-MM-dd").parse(row[4]);
                list.add(birthday.getTime()+"");

                Date creationDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S").parse(row[5]);

                list.add(creationDate.getTime()+"");

                list.add(row[6]);
                list.add(row[7]);
                list.add(row[8]);

            }
        } catch (Exception e){
            System.out.println("Customer issues");
            System.out.println(e);
        }
        System.out.println("done Customer");
    }

    public static void vendor(RedissonClient redisson){
        String file = "DATA/Vendor/Vendor.csv";
        String line;
        System.out.println("Vendor ...");
        RList<String> vendors = redisson.getList("Vendors");
        try (BufferedReader br =
                     new BufferedReader(new FileReader(file))) {
            br.readLine();
            while((line = br.readLine()) != null){

                String[] row = line.split(",");
                RMap<String, String> map = redisson.getMap(row[0]);
                map.put("Country",row[1]);
                map.put("Industry",row[2]);
                if(!row[1].equals("England")){
                    vendors.add(row[0]);
                }


            }
        } catch (Exception e){
            System.out.println("Vendor issues");
            System.out.println(e);
        }
        System.out.println("done Vendor");
    }

    public static void order(RedissonClient redisson){
        String file = "DATA/Order/Order.json";
        String line;
        System.out.println("Order ...");


    
        System.out.println("done Order");
    }
}
