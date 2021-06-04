package gmallsuger.demo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/4 10:19
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VisitorStats {
    private String hour;
    private String is_new;
    private Long uv_ct;
    private Long pv_ct;
    private Long uj_ct;
    private Long sv_ct;
}
