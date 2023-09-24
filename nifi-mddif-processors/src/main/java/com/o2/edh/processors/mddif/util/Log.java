package com.o2.edh.processors.mddif.util;

import org.apache.nifi.logging.LogLevel;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class Log {
    private LogLevel logLevel;
    private String logUUID;
    private String logTimestamp;
    private String confId;
    private String fileUUID;
    private String fileName;
    private String logTypeId;
    private String message;
    private static String ipAddress ;

    static {
        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
            ipAddress = ipAddress.substring(ipAddress.lastIndexOf('.') + 1);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.out.println("UnknownHostException: while initializing com.o2.edh.processors.mddif.util.Log class."+
                    e.getMessage());
        }
    }

    public Log(LogLevel logLevel, String confId, String fileUUID, String fileName, String logTypeId, String message) {
        this.logLevel = logLevel;
        this.confId = confId;
        this.fileUUID = fileUUID;
        this.fileName = fileName;
        this.logTypeId = logTypeId;
        this.message = message;
        this.logUUID = ipAddress + "-" + UUID.randomUUID().toString();
        this.logTimestamp = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(new Date());
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

    public String getLogUUID() {
        return logUUID;
    }

    public String getLogTimestamp() {
        return logTimestamp;
    }

    public String getConfId() {
        return confId;
    }

    public String getFileUUID() {
        return fileUUID;
    }

    public String getFileName() {
        return fileName;
    }

    public String getLogTypeId() {
        return logTypeId;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "Log{" +
                "logLevel=" + logLevel +
                ", logUUID='" + logUUID + '\'' +
                ", logTimestamp='" + logTimestamp + '\'' +
                ", confId='" + confId + '\'' +
                ", fileUUID='" + fileUUID + '\'' +
                ", fileName='" + fileName + '\'' +
                ", logTypeId='" + logTypeId + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
