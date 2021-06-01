package com.atguigu.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/1 14:32
 */
public class MyKeyWordUtil {
    
    public static Set<String> analyzer(String text) {
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
    
        Set<String> result = new HashSet<>();
        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                result.add(word);
                next = ikSegmenter.next();;
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return result;
    }
}
