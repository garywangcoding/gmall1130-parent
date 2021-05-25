import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/25 10:34
 */
public class LD {
    public static void main(String[] args) {
        LocalDate parse = LocalDate.parse("2021-05-25").plusDays(1);
    
        LocalDateTime ld = LocalDateTime.of(parse, LocalTime.of(0, 0, 0));
        System.out.println(ld);
    
    }
}
