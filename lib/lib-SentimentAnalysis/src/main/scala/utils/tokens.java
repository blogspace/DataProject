package utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


import net.paoding.analysis.analyzer.PaodingAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

public class tokens {
    public static List<String> anaylyzerWords (String str){
        // TODO Auto-generated method stub
        //定义一个解析器
        Analyzer analyzer = new PaodingAnalyzer();
        //定义一个存放存词的列表
        List<String> list=new ArrayList<String>();
        //得到token序列的输出流
        TokenStream tokens = analyzer.tokenStream(str, new StringReader(str));
        try{
            Token t;
            while((t=tokens.next() ) !=null){
                list.add(t.termText());
            }
        }catch(IOException e){
            e.printStackTrace();
        }
        return list;
    }
}
