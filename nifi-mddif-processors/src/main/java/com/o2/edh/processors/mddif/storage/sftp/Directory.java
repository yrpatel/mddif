package com.o2.edh.processors.mddif.storage.sftp;

import java.util.ArrayList;

public class Directory {
    String directoryPath;
    public String getDirectoryPath() {
        return directoryPath;
    }

    public void setDirectoryPath(String directoryPath) {
        this.directoryPath = directoryPath;
    }

    String supportEmailList;
    ArrayList<String> fileRegexList;

    public ArrayList<String> getFileRegexList() {
        return fileRegexList;
    }

    public void setFileRegexList(ArrayList<String> fileRegexList) {
        this.fileRegexList = fileRegexList;
    }

    public String getSupportEmailList() {
        return supportEmailList;
    }

    public void setSupportEmailList(String supportEmailList) {
        this.supportEmailList = supportEmailList;
    }

    public String getQuarantineDirectory() {
        return quarantineDirectory;
    }

    public void setQuarantineDirectory(String quarantineDirectory) {
        this.quarantineDirectory = quarantineDirectory;
    }

    String quarantineDirectory;

    public String getUsername() {
        return username;
    }

    String privateKeyPath;
    String project;

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getPrivateKeyPath() {
        return privateKeyPath;
    }

    public void setPrivateKeyPath(String privateKeyPath) {
        this.privateKeyPath = privateKeyPath;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    String username;

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    String hostname;

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    String port;
}
