import com.sun.tools.javac.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LineLogModel implements java.io.Serializable {
    public LineLogModel() {}
    public LineLogModel(Map<String, Tag> tags, String s) {
        init(tags, s);
    }

    /**
     * 按列分割模型, 不存在的标签默认识别为key型
     * @param tags 标签列表
     * @param s    模型描述字符串, 例如: "%{Referer}>h" "%{User-Agent}>h" %tr %<sb
     */
    void init(Map<String, Tag> tags, String s) {
        line = new ArrayList<Tag>();
        char [] cs = s.toCharArray();
        Tag tag;
        int idx = 0;
        do {
            tag = getNextConst(cs, idx);
            idx += tag.getName().length();
            line.add(tag);
            tag = getNextTag(tags, cs, idx);
            if (tag != null) {
                idx += tag.getName().length();
                line.add(tag);
            }
        }while(tag != null);
    }

    /**
     * 根据模型解析行数据, 实例化一个行数据对象
     * @param s 原始行字符串
     * @return LineData 行数据对象
     */
    public LineData parse(String s) {
        LineData ld = new LineData();
        Tag tag = line.get(0);

        String str = s.trim();
        if (!str.startsWith(tag.getName())) {
            return null;
        }
        int idx = tag.getName().length();
        int end;
        for (int i = 1; i < line.size(); i += 2) {
            tag = line.get(i + 1);
            end = str.indexOf(tag.getName(), idx);
            if (end < 0) {
                return null;
            }
            if (i + 1 == line.size() - 1) {
                ld.set(line.get(i), str.substring(idx));
            }
            else {
                ld.set(line.get(i), str.substring(idx, end));
            }
            idx = end + tag.getName().length();
        }
        return ld;
    }

    public String generate(LineData ld) {
        String s = "";
        Tag tag;
        Map<String, TagValue> data = ld.getData();
        for (int i = 0; i < line.size(); i++) {
            tag = line.get(i);
            if (tag.getType() != Tag.TagType.TagConst)
                s += data.get(tag.getName()).toString();
            else
                s += tag.getName();
        }
        return s;
    }

    private Tag getNextConst(char[] s, int idx) {
        int end = idx;
        while (end < s.length && s[end] != '%')
            end++;
        Tag tag = new Tag(Tag.TagType.TagConst, new String(s, idx, end - idx));
        return tag;
    }

    private Tag getNextTag(Map<String, Tag> tags, char[] s, int idx) {
        int end = idx + 1;
        if (idx >= s.length || s[idx] != '%')
            return null;
        if (s[end] == '{') {
            while (end < s.length && s[end] != '}')end++;
            if (end >= s.length)
                return null;
            end++;
        }

        while (end < s.length && isTagChar[s[end]] == 1)end++;
        if (end == idx + 1)
            return null;
        String tagKey = new String(s, idx, end - idx);
        Tag tag = tags.get(tagKey);
        if (tag != null) {
            if (!tagKey.equals(tag.getName())) {
                System.err.println("tagKey: " + tagKey + " == " + tag.getName());
                Assert.check(tagKey.equals(tag.getName()));
            }
            return tag;
        }
        tag = new Tag(Tag.TagType.TagString, tagKey);
        return tag;
    }

    List<Tag> line;

    static char[] isTagChar = new char[256];
    static  {
        for (int i = 'a'; i <= 'z'; i++)
            isTagChar[i] = 1;
        for (int i = 'A'; i <= 'Z'; i++)
            isTagChar[i] = 1;
        for (int i = 0; i <= 9; i++)
            isTagChar[i] = 1;
        isTagChar['_'] = 1;
        isTagChar['>'] = 1;
        isTagChar['<'] = 1;
    }

    /**
     * test
     * @param args
     */
    public static void main(String[] args) {
        Map<String, Tag> tags = new HashMap<>();
        tags.put("%tr", new Tag(Tag.TagType.TagLong, "%tr"));
        tags.put("%<sb", new Tag(Tag.TagType.TagLong, "%<sb"));

        LineLogModel lm = new LineLogModel();
        lm.init(tags, "\"%{Referer}>h\" \"%{User-Agent}>h\" %tr %<sb");
        LineData ld = lm.parse("\"-\" \"wspoll(x86_64-redhat-linux-gnu)/1.0.0-2 OpenSSL/1.0.2l zlib/1.2.3\" 0 307200\n");
        LineData ld2 = lm.parse("\"http://d.app6.i4.cn/soft/2018/03/08/11/473225145\" \"Mozilla/4.0; .NET CLR 2.0.50727)\" 2 55");

        System.out.println(ld.toString());
        System.out.println(ld2.toString());
        ld.add(ld2);
        System.out.println(ld.toString());
    }
}
