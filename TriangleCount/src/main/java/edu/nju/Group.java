package edu.nju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Group {
    public static class GroupMapper extends Mapper<Object,Text,Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vertices = value.toString().split(" ");
            if(vertices.length == 2){
                context.write(new Text(vertices[0]),new Text(vertices[1]));
                context.write(new Text(vertices[1]),new Text(vertices[0]));
            }
            else{
                System.err.println("error format");
                System.exit(1);
            }
        }
    }

    public static class GroupReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            Map<String,Integer> map = new HashMap<String, Integer>();
            Iterator<Text> it = values.iterator();
            if(it.hasNext()){
                String neighbor = it.next().toString();
                map.put(neighbor,1);
                sb.append(neighbor);
            }
            while(it.hasNext()){
                String neighbor = it.next().toString();
                if(!map.containsKey(neighbor)) {
                    sb.append(",").append(neighbor);
                    map.put(neighbor,1);
                }
            }
            String neighbors = sb.toString();
            context.write(key,new Text(neighbors));
        }
    }

//    public static class GroupReducer extends Reducer<Text,Text,Text,Text>{
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            StringBuilder sb = new StringBuilder();
//            Iterator<Text> it = values.iterator();
//            if(it.hasNext()){
//                String neighbor = it.next().toString();
//                sb.append(neighbor);
//            }
//            while(it.hasNext()){
//                String neighbor = it.next().toString();
//                sb.append("#").append(neighbor);
//                //System.exit(0);
//            }
//            String neighbors = sb.toString();
//            context.write(key,new Text(neighbors));
//        }
//    }

    public static void main(String args[])throws Exception{
        Configuration conf = new Configuration();
        if (args.length < 2){
            System.err.println("Usage: hadoop jar TriangleCount.jar edu.nju.Group <inPath> <outPath>");
            System.exit(1);
        }
        Job job1 = Job.getInstance(conf, " Group");
        job1.setJarByClass(Group.class);
        job1.setMapperClass(GroupMapper.class);
        //job1.setCombinerClass(GroupCombiner.class);
        job1.setReducerClass(GroupReducer.class);
        job1.setNumReduceTasks(10);
        job1.setPartitionerClass(HashPartitioner.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = outputPath.getFileSystem(conf);

        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job1, outputPath);
        job1.waitForCompletion(true);
    }
}
