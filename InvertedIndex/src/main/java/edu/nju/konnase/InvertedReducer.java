package edu.nju.konnase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InvertedReducer
        extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        StringBuilder all = new StringBuilder();
        int sum = 0, count = 0;
        if (it.hasNext()) {
            String s = it.next().toString();
            int freq = Integer.parseInt(s.split(":")[1]);
            sum += freq;
            ++count;
            all.append(s);
        }
        while (it.hasNext()){
            String s = it.next().toString();
            int freq = Integer.parseInt(s.split(":")[1]);
            sum += freq;
            ++count;
            all.append(";");
            all.append(s);
        }
        String value = String.format("\t%.2f,%s", (float)sum/count, all.toString());
        String term = key.toString();
        context.write(new Text(term), new Text(value));
    }
}