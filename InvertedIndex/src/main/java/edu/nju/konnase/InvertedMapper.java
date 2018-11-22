package edu.nju.konnase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class InvertedMapper
        extends Mapper<Object, Text, Text, Text> {

    private Text keyword = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String initFileName = fileSplit.getPath().getName();
        String suffix = ".txt.segmented";
        String fileName = initFileName.substring(0, initFileName.length() - suffix.length());
        String word;
        HashMap<String, Integer> wordFreq = new HashMap<String, Integer>();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word = itr.nextToken();
            if (wordFreq.containsKey(word)){
                wordFreq.put(word, wordFreq.get(word) + 1);
            } else {
                wordFreq.put(word, 1);
            }
        }
        for (Map.Entry<String, Integer> entry : wordFreq.entrySet()){
            word = entry.getKey();
            int freq = entry.getValue();
            Text file_freq = new Text(fileName + ':' + freq);
            keyword = new Text(word);
            context.write(keyword, file_freq);
        }
    }
}