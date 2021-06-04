package gmallsuger.demo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/4 10:19
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProvinceStats {
    private String province_name;
    private BigDecimal order_amount;
    private Long order_count;
}
