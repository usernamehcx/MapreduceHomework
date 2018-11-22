package edu.nju;


import org.apache.hadoop.conf.Configuration;
import java.util.Scanner;
import java.io.IOException;
import java.io.File;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem; import org.apache.hadoop.fs.Path;


public class Driver {

    public static void main(String[] args)throws Exception{
        long start_time = System.currentTimeMillis();

        Group group = new Group();
        Triangle triangle = new Triangle();
        Sum sum = new Sum();

        if (args.length < 2){
            System.err.println("Usage: hadoop jar TriangleCount.jar edu.nju.Driver <inPath> <outPath1> <outPath2> <outPath3>");
            System.exit(1);
        }

        String[] args1 = {args[0], args[1]};
        String[] args2 = {args[1], args[2]};
        String[] args3 = {args[2], args[3]};
        group.main(args1);
        triangle.main(args2);
        sum.main(args3);

        long end_time = System.currentTimeMillis();
        long time = (end_time - start_time) / 1000;

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        String ouput = args[3] + "/part-r-00000";
        try{
            FSDataInputStream in = hdfs.open(new Path(ouput));
            Scanner scan = new Scanner(in);
            while(scan.hasNext()) {
                String str = scan.nextLine();
                System.out.println(str);
            }
            scan.close();
        }
        catch(IOException ioe){
            ioe.printStackTrace();
        }
        System.out.println("run time :" + time + "s");
    }
}
