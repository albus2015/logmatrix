import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 支持按列分割的行数据模型
 */
public class LineModel implements java.io.Serializable {
    public LineModel() {}
    public LineModel(Map<String, Tag> tags, String s, String sp) {
        init(tags, s, sp);
    }

    /**
     * 按列分割模型, 不存在的标签默认识别为key型
     * @param tags 标签列表
     * @param s    模型描述字符串, 例如: %channel %flow %visit
     * @param sp   s的分隔符
     */
    void init(Map<String, Tag> tags, String s, String sp) {
        String[] ar = s.split(sp);
        line = new ArrayList<Tag>();
        for (int i = 0; i < ar.length; i++) {
            if (tags.get(ar[i]) != null)
                line.add(tags.get(ar[i]));
            else
                line.add(new Tag(Tag.TagType.TagString, ar[i]));
        }
        this.sp = sp;
    }

    /**
     * 根据模型解析行数据, 实例化一个行数据对象
     * @param s 原始行字符串
     * @return LineData 行数据对象
     */
    public LineData parse(String s) {
        LineData ld = new LineData();
        String[] ar = s.trim().split(sp);
        for (int i = 0; i < ar.length && i < line.size(); i++) {
            ld.set(line.get(i), ar[i]);
        }
        return ld;
    }

    List<Tag> line;
    private String sp;

    /**
     * test
     * @param args
     */
    public static void main(String[] args) {
        Map<String, Tag> tags = new HashMap<>();
        tags.put("%flow", new Tag(Tag.TagType.TagLong, "%flow"));
        tags.put("%visit", new Tag(Tag.TagType.TagLong, "%visit"));

        LineModel lm = new LineModel(tags, "%channel %flow %visit", " ");
        LineData ld = lm.parse("test.com 123 17\n");
        LineData ld2 = lm.parse("test.com 11 4");

        System.out.println(ld.toString());
        System.out.println(ld2.toString());
        ld.add(ld2);
        System.out.println(ld.toString());
    }
}
