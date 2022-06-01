package bigdata;

import org.redisson.Redisson;
import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.client.codec.StringCodec;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

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
}
