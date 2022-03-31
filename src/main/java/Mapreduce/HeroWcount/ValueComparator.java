package Mapreduce.HeroWcount;

import java.util.Comparator;
import java.util.Map;

public class ValueComparator implements Comparator<String> {

    Map<String, Integer> base;
    public ValueComparator(Map<String, Integer> base) {
        this.base = base;
    }
    public int compare(String o1, String o2) {
        if (o1!=null&&o2!=null) {
            int c1=base.get(o2).compareTo(base.get(o1));
            if (c1 == 0)
                return o1.compareTo(o2);
            return c1;
        }
        return 0;
    }
}