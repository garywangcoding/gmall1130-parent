import java.util.HashMap;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/24 9:32
 */
public class RemoveDemo {
    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put("a", "aa");
        map.put("aa", "aaa");
        map.put("b", "bbb");
        map.put("cc", "ccc");
        
        map.keySet().removeIf(k -> k.length() == 1);
        System.out.println(map);
    }
}
