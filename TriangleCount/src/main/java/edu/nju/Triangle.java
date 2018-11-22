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
import java.util.Map;
import java.util.StringTokenizer;

public class Triangle {
    public static class TriangleMapper extends Mapper<Object,Text,Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            String vertice1 = strs[0];
            String vertices = strs[1];
            //String[] temp = vertices.split(",");
            StringTokenizer stringTokenizer = new StringTokenizer(vertices,",");
            while(stringTokenizer.hasMoreTokens()){
                String vertice2 = stringTokenizer.nextToken();
                String k = sortedKey(vertice1,vertice2);
                context.write(new Text(k),new Text(vertices));
            }
        }

        public static String sortedKey(String v1, String v2){
            //return v1.compareTo(v2)?(v1+","+v2):(v2+","+v1);
            if(v1.compareTo(v2) > 0){
                return v1 + "," + v2;
            }
            else{
                return v2 + "," + v1;
            }
        }

    }

//    public static class TriangleCombiner extends Reducer<Text,Text,Text,Text>{
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//
//        }
//    }

    public static class TriangleReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] vertices = key.toString().split(",");
            String v1 = vertices[0];
            String v2 = vertices[1];

            Map<String,Integer> map = new HashMap<String, Integer>();
            for(Text val : values){
                String[] neighbors = val.toString().split(",");
                for(String neighbor : neighbors) {
                    if (map.containsKey(neighbor)) {
                        int count = map.get(neighbor);
                        map.put(neighbor, ++count);
                    } else {
                        if (!neighbor.equals(v1) && !neighbor.equals(v2))
                            map.put(neighbor, 1);
                    }

                }
            }

            for(Map.Entry<String,Integer> entry : map.entrySet()){
                if(entry.getValue() >= 2){
                    context.write(new Text(v1+","+v2+","+entry.getKey()),new Text("1"));
                }
            }

        }
    }
    public static void main(String args[])throws Exception{
        Configuration conf = new Configuration();
        if (args.length < 2){
            System.err.println("Usage: hadoop jar TriangleCount.jar edu.nju.Group <inPath> <outPath>");
            System.exit(1);
        }
        Job job2 = Job.getInstance(conf, "triangle");
        job2.setJarByClass(Group.class);
        job2.setMapperClass(TriangleMapper.class);
        //job2.setCombinerClass(TriangleCombiner.class);
        job2.setReducerClass(TriangleReduce.class);
        job2.setNumReduceTasks(10);
        job2.setPartitionerClass(HashPartitioner.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = outputPath.getFileSystem(conf);

        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job2, outputPath);
        job2.waitForCompletion(true);
    }
}


//18/11/21 06:46:08  18/11/21 06:27:12
