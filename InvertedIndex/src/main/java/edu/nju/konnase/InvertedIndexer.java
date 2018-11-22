package edu.nju.konnase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndexer {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        if (args.length < 2){
            System.err.println("Usage: hadoop jar InvertedIndex.jar edu.nju.konnase.InvertedIndexer <inPath> <outPath>");
            System.exit(1);
        }
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndexer.class);
        job.setMapperClass(InvertedMapper.class);
        job.setCombinerClass(InvertedCombiner.class);
        job.setReducerClass(InvertedReducer.class);
        job.setNumReduceTasks(2);
        job.setPartitionerClass(HashPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}