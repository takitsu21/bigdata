package bigdata;

import java.util.*;

import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

public class Utils {
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
            threeHop.addAll(Utils.getHopThree(redisson, person, depth - 1, seen));
        }
        return threeHop;
    }

    public static List<String> getHopThreePath(RedissonClient redisson, String personIdFrom, String personIdTo) {
        Set<String> seen = new HashSet<>();
        Map<String, String> prev = new HashMap<>();
        List<String> directions = new LinkedList<>();
        Queue<String> q = new LinkedList<>();
        StringCodec codec = new StringCodec();

        String current = personIdFrom;
        q.add(current);
        seen.add(current);
        while (!q.isEmpty()) {
            current = q.remove();
            if (current.equals(personIdTo)) {
                break;
            } else {
                RList<String> knows = redisson.getList(current + "_Knows", codec);
                for (String person : knows) {
                    if (!seen.contains(person)) {
                        q.add(person);
                        seen.add(person);
                        prev.put(person, current);
                    }
                }
            }
        }
        if (!current.equals(personIdTo)) {
            return directions;
        }
        for (String person = personIdTo; person != null; person = prev.get(person)) {
            if (person != personIdFrom && person != personIdTo) {
                directions.add(person);
            }
        }
        Collections.reverse(directions);
        return directions;
    }

    public static class Company {
        int sales = 0;
        int males = 0;
        int females = 0;
        Set<String> customers = new HashSet<>();
        Set<String> posts = new HashSet<>();

        @Override
        public String toString() {
            return "\nsales: " + sales + "\nmales: " + males + "\nfemales: " + females + "\ncustomers: "
                    + customers.size() + "\n";
        }
    }

    public static class RFM {
        int recency = 0;
        int frequency = 0;
        int monetary = 0;
        Set<String> tags = new HashSet<>();
        Set<String> reviews = new HashSet<>();

        @Override
        public String toString() {
            return "\nrecency: " + recency + "\nfrequency: " + frequency + "\nmonetary: " + monetary + "\n";
        }
    }

    public static Object getMaxFromMap(Map<?, Integer> map) {
        int maxEntry = 0;
        Object ret = null;
        for (Map.Entry<?, Integer> entry : map.entrySet()) {
            if (maxEntry < entry.getValue()) {
                maxEntry = entry.getValue();
                ret = entry.getKey();
            }
        }
        return ret;
    }

    public static class MapPrinter<K, V> {

        private Map<K, V> map;

        public MapPrinter(Map<K, V> map) {
            this.map = map;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<K, V> entry = iter.next();
                sb.append(entry.getKey());
                sb.append('=').append('"');
                sb.append(entry.getValue());
                sb.append('"');
                if (iter.hasNext()) {
                    sb.append(',').append(' ');
                }
                sb.append("\n");
            }
            return sb.toString();
        }
    }

}
