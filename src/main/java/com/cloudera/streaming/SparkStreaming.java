package com.cloudera.streaming;


import info.goodline.model.RdrParser;
import info.goodline.model.RdrRaw;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.File;
import java.util.regex.Pattern;


/**
 * Created by sergeyb on 21.12.15.
 */
public class SparkStreaming {

    private static final Pattern SPACE = Pattern.compile(" ");

    /**
     * @param args args[0] path to directory
     */
    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: SparkStreaming <stream folder>");
            System.exit(1);
        }
        String streamFolder = args[0];
        System.out.println("stream folder [" + args[0] + "]");
        //System.out.println("stream folder contains files ["+getFilesCount(f)+"]");
        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[4]");
        System.out.println("trace 1");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        System.out.println("trace 2");
        JavaDStream<String> stringJavaDStream = ssc.textFileStream(streamFolder);
        System.out.println("trace 3");
        //stringJavaDStream.persist();
        //stringJavaDStream.compute(new Time(10000));
        System.out.println("trace 4");
        stringJavaDStream.print(10);
        JavaDStream<String> javaDStream = stringJavaDStream.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String value) throws Exception {
                if (value.contains("TIME_STAMP")) {
                    return false;
                }
                System.out.println("-----------------------" + value);
                RdrRaw line = RdrParser.parseRdr(value);
                if (line == null) {
                    System.out.println("can't pars rdr");
                    return false;
                }
                String url = line.dstHost + line.dstParam;

                return !url.trim().isEmpty();
            }
        });
        javaDStream.print(10);
        System.out.println("trace 5");

        System.out.println("trace 6");
        Duration d = new Duration(5000);
        javaDStream.checkpoint(d);
        ssc.checkpoint("hdfs:///spark/checkpoint");
      /*  javaDStream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>("",s);
            }
        }).saveAsNewAPIHadoopFiles("","",Text.class,Text.class, org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class,new Configuration());
*/
        ssc.start();
        ssc.awaitTermination();
    }

    public static int getFilesCount(File file) {
        File[] files = file.listFiles();
        int count = 0;
        for (File f : files)
            if (f.isDirectory())
                count += getFilesCount(f);
            else
                count++;

        return count;
    }

}

