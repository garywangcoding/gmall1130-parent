package com.atguigu.realtime.app.function;

import com.atguigu.realtime.common.Constant;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/1 14:27
 */
@FunctionHint(output = @DataTypeHint("ROW<source string, ct bigint>"))
public class KeyWordProductUdtf extends TableFunction<Row> {
    public void eval(Long clickCt, Long orderCt, Long cartCt) {
        if (clickCt > 0) {
            collect(Row.of(Constant.SOURCE_CLICK, clickCt));
        }
        if (orderCt > 0) {
            collect(Row.of(Constant.SOURCE_ORDER, orderCt));
        }
        
        if (cartCt > 0) {
            collect(Row.of(Constant.SOURCE_CART, cartCt));
        }
        
    }
}
