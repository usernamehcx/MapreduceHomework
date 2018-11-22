package njuhcx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class TotalSort {
    public static class TotalSortMapper extends Mapper<Object,Text,Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split("\t");
//            String word = str[0];
//            String average = str[1];
//            String freq = str[2];
            String[] temp = str[1].split(",");
            context.write(new Text(temp[0]),new Text(str[0]+"#"+temp[1]));
        }
    }



    public static class TotalSortReduce extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException{
            Iterator<Text> it = values.iterator();
            while (it.hasNext()){
                String[] strs = it.next().toString().split("#");
                context.write(new Text(strs[0]),new Text(key.toString()+","+strs[1]));

            }
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        if (args.length < 2){
            System.err.println("Usage: hadoop jar InvertedIndex.jar njuhcx.TotalSort <inPath> <outPath>");
            System.exit(1);
        }
        Job job = Job.getInstance(conf, "total sort");
        job.setJarByClass(TotalSort.class);
        job.setMapperClass(TotalSortMapper.class);
        //job.setCombinerClass(TotalSortReduce.class);
        job.setReducerClass(TotalSortReduce.class);
        job.setNumReduceTasks(1);
        job.setPartitionerClass(HashPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
