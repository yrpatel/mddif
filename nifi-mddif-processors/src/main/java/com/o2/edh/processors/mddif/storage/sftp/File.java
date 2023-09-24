package com.o2.edh.processors.mddif.storage.sftp;

public class File {
    private String fileName = null;
    private Long fileSize = null;
    private String lastModifiedDatetime = null;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }

    public String getLastModifiedDatetime() {
        return lastModifiedDatetime;
    }

    public void setLastModifiedDatetime(String lastModifiedDatetime) {
        this.lastModifiedDatetime = lastModifiedDatetime;
    }
}
