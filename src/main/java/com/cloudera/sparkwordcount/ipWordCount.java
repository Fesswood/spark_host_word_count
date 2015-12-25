/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sparkwordcount;

import info.goodline.model.RdrParser;
import info.goodline.model.RdrRaw;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class ipWordCount {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf()
                .set("spark.dynamicAllocation.initialExecutors", "5")
                .setAppName("Spark Count"));
        // sc.addJar("");
        //   final Logger logger = Logger.getLogger("org");
        // logger.setLevel(Level.INFO);
        final int threshold = Integer.parseInt(args[1]);
        JavaRDD<String> stringJavaRDD = sc.textFile(args[0]);
        JavaRDD<String> filteredRDD = stringJavaRDD.filter(new Function<String, Boolean>() {
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
                //System.out.println(url);
                return true;
            }
        });
        JavaPairRDD<RdrRaw, Integer> countsIp = filteredRDD.mapToPair(new PairFunction<String, RdrRaw, Integer>() {
            @Override
            public Tuple2<RdrRaw, Integer> call(String s) throws Exception {
                RdrRaw rdrRaw = RdrParser.parseRdr(s);
                return new Tuple2<RdrRaw, Integer>(rdrRaw, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        // filter out words with less than threshold occurrences
        JavaPairRDD<RdrRaw, Integer> filtered
                = countsIp.filter(new Function<Tuple2<RdrRaw, Integer>, Boolean>() {
                                      @Override
                                      public Boolean call(Tuple2<RdrRaw, Integer> rdrRawIntegerTuple2) throws Exception {
                                          return rdrRawIntegerTuple2._2() > threshold;
                                      }
                                  }
        );
        JavaPairRDD<Integer, RdrRaw> finalPair = filtered.mapToPair(new PairFunction<Tuple2<RdrRaw, Integer>, Integer, RdrRaw>() {
            @Override
            public Tuple2<Integer, RdrRaw> call(Tuple2<RdrRaw, Integer> item) throws Exception {
                return item.swap();
            }
        }).sortByKey(false);
        //
        List<Tuple2<Integer, RdrRaw>> collect = finalPair.take(10);
        StringBuilder msgBody = new StringBuilder();
        for (Tuple2<Integer, RdrRaw> rdrInTuple2 : collect) {
            RdrRaw rdrRaw = rdrInTuple2._2();
            Integer count = rdrInTuple2._1();
            msgBody.append(rdrRaw.dstHost)
                    // .append(rdrRaw.dstParam)
                    .append(" found [")
                    .append(count)
                    .append("]\n");
        }
        Configuration conf = new Configuration();
        try {
            Path p = new Path(args[2]);
            FileSystem fs = FileSystem.get(new Configuration());
            boolean exists = fs.exists(p);
            if (exists) {
                fs.delete(p, true);
            }
            FileSystem hdfs = FileSystem.get(conf);
            FSDataOutputStream out = hdfs.create(p);
            ByteArrayInputStream in = new ByteArrayInputStream(msgBody.toString().getBytes());
            byte buffer[] = new byte[256];
            int bytesRead = 0;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
            p = new Path(args[2] + "_all");
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
            finalPair.saveAsTextFile(args[2] + "_all");
        } catch (IOException e) {
            e.printStackTrace();
        }

        sc.stop();
       /* Properties props = new Properties();
        props.put("mail.smtps.host","smtp.gmail.com");
        props.put("mail.smtps.auth", "true");
        Session session = Session.getDefaultInstance(props, null);

        System.out.println("try send email");
        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress("spark@hadoop.com", "Spark Generated Message"));
            msg.addRecipient(Message.RecipientType.TO,
                    new InternetAddress("fesswoodwork@gmail.com", "Spark Responder"));
            msg.setSubject("Spark task finished");
            msg.setText(msgBody.toString());
            SMTPTransport t =
                    (SMTPTransport)session.getTransport("smtps");
            t.connect("smtp.gmail.com", "fesswoodwork", "9610792adc");
            t.sendMessage(msg, msg.getAllRecipients());
            Transport.send(msg);

        } catch (AddressException e) {
           e.printStackTrace();
            System.out.println("AddressException "+e.getMessage());
        } catch (MessagingException e) {
            e.printStackTrace();
            System.out.println("MessagingException " + e.getMessage());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.out.println("UnsupportedEncodingException " + e.getMessage());
        }
        System.out.println("sending successfully ends");*/


  /*      // split each document into words
        JavaRDD<String> tokenized = stringJavaRDD.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );

        // count the occurrence of each word
        JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        // filter out words with less than threshold occurrences
        JavaPairRDD<String, Integer> filtered = counts.filter(
                new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> tup) {
                        return tup._2() >= threshold;
                    }
                }
        );

        // count characters
        JavaPairRDD<Character, Integer> charCounts = filtered.flatMap(
                new FlatMapFunction<Tuple2<String, Integer>, Character>() {
                    @Override
                    public Iterable<Character> call(Tuple2<String, Integer> s) {
                        Collection<Character> chars = new ArrayList<Character>(s._1().length());
                        for (char c : s._1().toCharArray()) {
                            chars.add(c);
                        }
                        return chars;
                    }
                }
        ).mapToPair(
                new PairFunction<Character, Character, Integer>() {
                    @Override
                    public Tuple2<Character, Integer> call(Character c) {
                        return new Tuple2<Character, Integer>(c, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        System.out.println(charCounts.collect());
        */

    }

    private static void logDebug(Logger logger, String url) {
        if (logger.isDebugEnabled()) {
            logger.debug("result url [" + url + "]");
        }
    }
}
