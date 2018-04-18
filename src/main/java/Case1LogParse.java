import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class Case1LogParse {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("myTest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, Tag> tags = new HashMap<>();
        tags.put("%<st", new Tag(Tag.TagType.TagLong, "%<st"));
        LineLogModel lm = new LineLogModel();
        lm.init(tags, "%>a %ui %un [%tl] \"%rm %ru HTTP/%rv\" %Hs %<st \"%{Referer}>h\" \"%{User-Agent}>h\" %tr %<sb \"%{Content-Length}<h\" \"%{Range}>h\" %Ss %Sh %rb %<a %hc_tag %lp %drs Via:\"%{Via}>h\" %lcse %reqid %{X-Request-Id}>h");

        JavaRDD<String> lines = sc.textFile("/Users/linzy/tmp/2018-04-10-10-56-00-5364_SR-CNCT-SNXY-185-135_dc4179d90751a6c551e1feb3751ae158.1.log.gz");

        JavaRDD<String> filter = lines.filter(s -> !s.startsWith("#"));

        JavaRDD<LineData> tr = filter.map(s -> lm.parse(s).reduce("%ru %<st"));

        JavaPairRDD<String, LineData> ones = tr.mapToPair(ld -> ld.getTuple2());

        JavaPairRDD<String, LineData> counts = ones.reduceByKey((i1, i2) -> i1.add(i2));

        List<Tuple2<String, LineData>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + "=> " + tuple._2().toString());
        }

        //System.out.println("Hello World! count = " + count);
    }
}
