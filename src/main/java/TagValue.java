public class TagValue implements java.io.Serializable {
    Tag tag;
    String value_s;
    long value_l;

    TagValue(Tag tag){
        this.tag = tag;
        value_s = "-";
        value_l = 0;
    }

    public TagValue dup() {
        TagValue ret = new TagValue(this.tag);
        ret.value_s = this.value_s;
        ret.value_l = this.value_l;
        return ret;
    }

    public TagValue set(String s) {
        try {
            if (tag.getType() == Tag.TagType.TagLong)
                value_l = Long.parseLong(s);
            else
                value_s = s;
        } catch (Exception e) {
        }
        return this;
    }

    public TagValue add(TagValue tv) {
        if (tv != null && tag.getType() == Tag.TagType.TagLong)
            value_l += tv.value_l;
        return this;
    }

    public Tag.TagType getType() {
        return tag.getType();
    }

    public Tag getTag() {
        return tag;
    }

    public Long getLong() {
        if (tag.getType() == Tag.TagType.TagLong)
            return value_l;
        else
            return 0L;
    }

    @Override
    public String toString() {
        if (tag.getType() == Tag.TagType.TagLong)
            return String.format("%d", value_l);
        else
            return value_s;
    }
}
