package com.atguigu.realtime.app.function;

import com.atguigu.realtime.util.MyKeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/1 14:27
 */
@FunctionHint(output = @DataTypeHint("ROW<w string>"))
public class KeyWordUdtf extends TableFunction<Row> {
    public void eval(String text){
        Set<String> words = MyKeyWordUtil.analyzer(text);
        for (String word : words) {
            collect(Row.of(word));
        }
    }
}
