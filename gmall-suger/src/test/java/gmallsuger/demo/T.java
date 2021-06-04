package gmallsuger.demo;

import java.text.DecimalFormat;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/4 11:37
 */
public class T {
    public static void main(String[] args) {
        DecimalFormat df = new DecimalFormat("0000.00%");
        System.out.println(df.format(123.479));
        System.out.println(df.format(1));
        System.out.println(df.format(24516));
    }
}
