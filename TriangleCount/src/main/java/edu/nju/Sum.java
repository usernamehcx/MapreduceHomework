package edu.nju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;

public class Sum {
    public static class SumMapper extends Mapper<Object,Text,Text,IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("count"),new IntWritable(1));

        }
    }

    public static class SumCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += 1;
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static class SumReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable val : values){
                count += val.get();
                context.write(key, val);
            }
            count =count / 3;
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        if (args.length < 2){
            System.err.println("Usage: hadoop jar TriangleCount.jar edu.nju.Group <inPath> <outPath>");
            System.exit(1);
        }
        Job job3 = Job.getInstance(conf, " sum Count");
        job3.setJarByClass(Sum.class);
        job3.setMapperClass(SumMapper.class);
        job3.setCombinerClass(SumCombiner.class);
        job3.setReducerClass(SumReduce.class);
        job3.setPartitionerClass(HashPartitioner.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job3, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = outputPath.getFileSystem(conf);

        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job3, outputPath);
       job3.waitForCompletion(true);
    }
}
