import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 行数据对象
 */
public class LineData implements java.io.Serializable {
    final static String spliceStr = "||";
    public LineData() {
        data = new HashMap<String, TagValue>();
    }

    public void set(Tag tag, String s) {
        if (!data.containsKey(tag.getName())) {
            data.put(tag.getName(), new TagValue(tag));
        }
        data.get(tag.getName()).set(s);
    }

    public Tuple2 getTuple2() {
        String s = "";
        for (Map.Entry<String, TagValue> entry : data.entrySet()) {
            if (entry.getValue().getType() == Tag.TagType.TagString)
                s += entry.getValue().toString() + spliceStr;
        }
        return new Tuple2<>(s, this);
    }

    /**
     * 数据列之间的加法, ret = tag1 + tag2
     * @param s 描述加法的字符串, 用空格隔开, 例如"tag1 tag2 ret", 或者"tag1 tag2"
     *          如果s有3个标签, 则和数标签是最后一个, 即最后一个标签的值等于前两个标签的和
     *          如果s有2个标签, 则将第二个标签的值加到第一个标签上
     * @return
     */
    public LineData merge(String s) {
        String[] ar = s.split(" ");
        if (ar.length == 2)
            return merge(ar[0], ar[1], ar[0]);
        else if (ar.length == 3)
            return merge(ar[0], ar[1], ar[2]);
        return this;
    }

    /**
     * 数据列之间的加法, ret = tag1 + tag2
     * @param tag1
     * @param tag2
     * @param ret
     * @return
     */
    public LineData merge(String tag1, String tag2, String ret) {
        TagValue v1 = data.get(tag1);
        TagValue v2 = data.get(tag2);
        if (v1 != null) {
            data.put(ret, v1.dup().add(v2));
        }
        else if (v2 != null) {
            data.put(ret, v2.dup());
        }
        return this;
    }

    /**
     * 降维操作, s是降维后的维度说明, 返回一个新的LineData对象
     * @param s 降维后的维度说明, 按空格隔开 例如: %channel %flow
     * @return
     */
    public LineData reduce(String s) {
        List<String> f = new ArrayList<>();
        String[] ar = s.split(" ");
        for (int i = 0; i < ar.length; i++) {
            f.add(ar[i]);
        }
        return reduce(f);
    }

    /**
     * 降维操作, f提供了降维后的维度说明, 返回一个新的LineData对象
     * @param f
     * @return
     */
    public LineData reduce(List<String> f) {
        LineData ld = new LineData();
        String key;
        for (int i = 0; i < f.size(); i++) {
            key = f.get(i);
            ld.data.put(key, data.get(key));
        }
        return ld;
    }

    /**
     * 根据配置文件进行扩维操作
     * @param mapConf
     * @param mapKey
     * @param mapValue
     * @return
     */
    public LineData map(Map<String, LineData> mapConf, List<String> mapKey, List<String> mapValue) {
        String s = "";
        for (int i = 0; i < mapKey.size(); i++) {
            s += mapKey.get(i) + spliceStr;
        }

        LineData ld = mapConf.get(s);
        if (ld == null)
            return this;
        for (int i = 0; i < mapValue.size(); i++) {
            data.put(mapValue.get(i), ld.data.get(mapValue.get(i)));
        }
        return this;
    }

    /**
     * 数据相加, this + l2, 生成一个新的对象, key型Tag和this保持一致, val型Tag等于this->v + l2->v
     * @param l2
     * @return LineData
     */
    public LineData add(LineData l2) {
        for (Map.Entry<String, TagValue> entry : data.entrySet()) {
            entry.getValue().add(l2.data.get(entry.getKey()));
        }
        return this;
    }

    @Override
    public String toString() {
        String s = "";
        for (TagValue tv : data.values()) {
            s += tv.toString() + " ";
        }
        return s;
    }

    public Map<String, TagValue> getData() {
        return data;
    }

    Map<String, TagValue> data;
}
