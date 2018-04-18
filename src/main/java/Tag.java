public class Tag implements java.io.Serializable {
    enum TagType {
        TagString,
        TagLong,
        TagConst,
        TagGroup,
        TagMax
    } ;
    public Tag(TagType type, String name){
        this.type = type;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    String name;

    public TagType getType() {
        return type;
    }

    TagType type;
}
