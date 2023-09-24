package com.o2.edh.processors.mddif.partition;

public class PartitionObj{
    private String columnName;
    private String source;
    private String format;
    private String appendMod;
    private String attributeName;

    public PartitionObj(String columnName, String source, String format, String appendMod, String attributeName) {
        this.columnName = columnName;
        this.source = source;
        this.format = format;
        this.appendMod = appendMod;
        this.attributeName = attributeName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getAppendMod() {
        return appendMod;
    }

    public void setAppendMod(String appendMod) {
        this.appendMod = appendMod;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    @Override
    public String toString() {
        return "PartitionObj{" +
                "columnName='" + columnName + '\'' +
                ", source='" + source + '\'' +
                ", format='" + format + '\'' +
                ", appendMod='" + appendMod + '\'' +
                ", attributeName='" + attributeName + '\'' +
                '}';
    }
}
