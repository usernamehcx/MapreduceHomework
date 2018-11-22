package edu.nju.konnase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedCombiner extends Reducer<Text,Text,Text,Text> {

    //合并mapper函数的输出
    protected void reduce(Text key,Iterable<Text> values,Context context)
            throws IOException,InterruptedException{

        String fileName="";
        int sum=0;
        String num;
        String s;
        for (Text val : values) {

            s= val.toString();
            fileName=s.split(":")[0];
            num=s.split(":")[1]; //提取“doc1@1”中‘@’后面的词频
            sum+=Integer.parseInt(num);
        }
        IntWritable frequence=new IntWritable(sum);
        String item = key.toString();
        context.write(new Text(item),new Text(fileName+":"+frequence.toString()));
    }
}