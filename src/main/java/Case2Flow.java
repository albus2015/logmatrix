/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class Case2Flow {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("myTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, Tag> tags = new HashMap<>();
        tags.put("$flow", new Tag(Tag.TagType.TagLong, "$flow"));
        tags.put("$post_flow", new Tag(Tag.TagType.TagLong, "$post_flow"));
        tags.put("$visit", new Tag(Tag.TagType.TagLong, "$visit"));
        LineModel lm = new LineModel();
        lm.init(tags, "$channel $flow $post_flow $visit", " ");

        JavaRDD<String> lines = sc.textFile("/Users/linzy/tmp/2018-04-09-09-53-00_3234_1523238720_5ddfc532ba566dde4befb73873acc525.ngFlow");

        JavaRDD<String> filter = lines.filter(s -> !s.startsWith("#"));

        JavaPairRDD<String, LineData> ones = filter.mapToPair(s -> lm.parse(s).reduce("$channel $flow $visit").merge("$flow $visit $new_flow").getTuple2());

        JavaPairRDD<String, LineData> counts = ones.reduceByKey((i1, i2) -> i1.add(i2));

        List<Tuple2<String, LineData>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + "=> " + tuple._2().toString());
        }

        /*****************************/
        JavaPairRDD<String, Long> zone = counts.mapValues(x -> x.getData().get("$flow").getLong());
        List<Tuple2<String, Long>> output2 = zone.collect();
        for (Tuple2<?,?> tuple : output2) {
            System.out.println(tuple._1() + "=> " + tuple._2().toString());
        }

        //System.out.println("Hello World! count = " + count);
    }
}