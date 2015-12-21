package com.cloudera.streaming;


import info.goodline.model.RdrParser;
import info.goodline.model.RdrRaw;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

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
        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreaming");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        JavaDStream<String> stringJavaDStream = ssc.textFileStream(streamFolder);
        JavaDStream<String> javaDStream = stringJavaDStream.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String value) throws Exception {
                if (value.contains("TIME_STAMP")) {
                    return false;
                }
                RdrRaw line = RdrParser.parseRdr(value);
                if (line == null) {
                    System.out.println("can't pars rdr");
                    return false;
                }
                String url = line.dstHost;
                if (url.trim().isEmpty()) {
                    return false;
                }
                String dstParam = line.dstParam;
                return !dstParam.trim().isEmpty();
            }
        });
        javaDStream.persist();
        javaDStream.compute(new Time(10000));
        ssc.start();
        ssc.awaitTermination();
    }
}

