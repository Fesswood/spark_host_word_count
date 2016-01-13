package com.cloudera.streaming;


import info.goodline.model.RdrRaw;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;


/**
 * Created by sergeyb on 21.12.15.
 */
public class SparkStreaming {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
    private static final Duration SLIDE_INTERVAL = new Duration(2 * 1000);
    private static final long MIN_FILE_SIZE = 1600000;
    private static HashMap<Long, File> fileHashMap = new HashMap<>();

    /**
     * @param args args[0] path to directory
     */
    public static void main(final String[] args) throws IOException {

        if (args.length < 1) {
            System.err.println("Usage: SparkStreaming <stream folder>");
            System.exit(1);
        }
        final String streamFolder = args[0];
        System.out.println("stream folder [" + args[0] + "] version  0.3");
        String streamOutPutFolder = args[1];
        System.out.println("stream output folder [" + args[1] + "] version  0.3");
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreaming");
        System.out.println("trace 1");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));
        System.out.println("trace 2");

        final JavaDStream<File> stringJavaDStream = ssc.receiverStream(new JavaCustomReceiver(args[0]));
        JavaDStream<File> javaDStream = stringJavaDStream.filter(new Function<File, Boolean>() {
            @Override
            public Boolean call(File value) throws Exception {
                System.out.println("new file arrived with name [" + value.getName() + "]" + " and size [" + value.length() + "]");
                boolean isFileCompletelyRecordered = true;// value.length() > MIN_FILE_SIZE;
                boolean isFileNew = !fileHashMap.containsKey(value.lastModified());
                System.out.println("isFileCompletelyRecordered = " + isFileCompletelyRecordered);
                System.out.println("isFileNew = " + isFileNew);
                if (isFileNew) {
                    fileHashMap.put(value.lastModified(), value);
                }
                return isFileCompletelyRecordered && isFileNew;
            }
        }).cache();
        JavaDStream<String> rdrRawStream = javaDStream.flatMap(new FlatMapFunction<File, String>() {
            @Override
            public Iterable<String> call(File file) throws Exception {
                System.out.println("start parse rdrRawStream");
                System.out.println("file hashMap" + fileHashMap.size());
                ArrayList<String> rdrList = new ArrayList<String>();
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    for (String line; (line = br.readLine()) != null; ) {
                        // process the line.
                        if (line.contains("TIME_STAMP")) {
                            continue;
                        }
                        System.out.println("line is " + line);
                        RdrRaw rdrRaw = RdrRaw.getInstance(line);
                        if (rdrRaw != null && !rdrRaw.dstHost.trim().isEmpty()) {
                            rdrList.add(rdrRaw.toStringLine());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("final rdrList size =" + rdrList.size());
                return rdrList;
            }
        });
        rdrRawStream.print();
        rdrRawStream.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
                //TODO save to file

                stringJavaRDD.saveAsTextFile(args[1]);
                return null;
            }
        });

        System.out.println("trace 6");
        Duration d = new Duration(60000);
        javaDStream.checkpoint(d);
        ssc.checkpoint("hdfs:///spark/checkpoint");

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

