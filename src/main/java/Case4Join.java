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

import java.util.*;
import java.util.regex.Pattern;

public final class Case4Join {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("myTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, Tag> tags = new HashMap<>();
        tags.put("%<st", new Tag(Tag.TagType.TagLong, "%<st"));
        tags.put("$body_bytes_sent", new Tag(Tag.TagType.TagLong, "$body_bytes_sent"));
        LineLogModel lm = new LineLogModel();
        lm.init(tags, "%>a %ui %un [%tl] \"%rm %ru HTTP/%rv\" %Hs %<st \"%{Referer}>h\" \"%{User-Agent}>h\" %tr %<sb \"%{Content-Length}<h\" \"%{Range}>h\" %Ss %Sh %rb %<a %hc_tag %lp %drs Via:\"%{Via}>h\" %lcse %reqid %{X-Request-Id}>h");

        JavaPairRDD<String, String> squid_log = sc.wholeTextFiles("/Users/linzy/tmp/squid/*.gz");
        JavaRDD<LineData> squid_lines = squid_log.flatMap(tuple2 -> {
            List<LineData> ret = new ArrayList<>();
            String[] ar = tuple2._2().split("\n");

            for (int i = 0 ; i < ar.length; i++) {
                if (!ar[i].startsWith("#"))
                    ret.add(lm.parse(ar[i]));
                else if (ar[i].startsWith("#Fields: "))
                    lm.init(tags, ar[i].substring(9));
            }
            return ret.iterator();
        });
        JavaPairRDD<String, Long> squid_size = squid_lines.mapToPair(ld -> new Tuple2<>(ld.getData().get("%>a").toString(), ld.getData().get("%<st").getLong()));
        JavaPairRDD<String, Long> squid_csize = squid_size.reduceByKey((i1, i2) -> i1 + i2);

        LineLogModel lm2 = new LineLogModel();
        lm2.init(tags, "$remote_addr - $remote_user [$time_local] \"$request\"  \"$scheme://$http_host\" $status $body_bytes_sent $request_time \"$http_referer\" \"$http_user_agent\" $ssl_protocol $ssl_cipher $upstream_addr $upstream_status $upstream_response_time $request_id $conn_state");
        JavaRDD<String> ng_lines = sc.textFile("/Users/linzy/tmp/nginx.log");
        JavaRDD<String> ng_filter = ng_lines.filter(s -> !s.startsWith("#"));
        JavaPairRDD<String, Long> ng_size = ng_filter.mapToPair(s -> {
            LineData ld = lm2.parse(s);
            return new Tuple2<>(ld.getData().get("$remote_addr").toString(), ld.getData().get("$body_bytes_sent").getLong());
        });
        JavaPairRDD<String, Long> ng_csize = ng_size.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairRDD<String, Tuple2<Long, Long>> ret = squid_csize.join(ng_csize);

        List<Tuple2<String, Tuple2<Long, Long>>> output = ret.collect();
        for (Tuple2<String, Tuple2<Long, Long>> tuple : output) {
            System.out.println(tuple._1() + " " + tuple._2().toString() + " " + tuple._2()._2());
        }

        //System.out.println("Hello World! count = " + count);
    }
}