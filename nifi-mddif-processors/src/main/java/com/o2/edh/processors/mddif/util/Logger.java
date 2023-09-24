package com.o2.edh.processors.mddif.util;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.util.HashMap;
import java.util.Map;

public class Logger {
    ComponentLog cLog;
    ProcessSession session;
    Relationship relationship;

    public Logger(ProcessSession session, ComponentLog cLog, Relationship relationship) {
        this.cLog = cLog;
        this.session = session;
        this.relationship = relationship;
    }

    public void generateLog(Log log){
        generateLog(log,this.relationship);
    }
    public void generateLog(Log log,Relationship relationship){
        generateLog(log, null, relationship);
    }
    public void generateLog(Log log,FlowFile flowFile){
        generateLog(log, flowFile, this.relationship);
    }

    public void generateLog(Log log,FlowFile flowFile, Relationship relationship){

        cLog.log(log.getLogLevel(),log.toString()); //App Log

        if (log.getLogLevel().equals(LogLevel.ERROR) && !cLog.isErrorEnabled())
            return;
        else if(log.getLogLevel().equals(LogLevel.WARN) && !cLog.isWarnEnabled())
            return;
        else if(log.getLogLevel().equals(LogLevel.INFO) && !cLog.isInfoEnabled())
            return;
        else if(log.getLogLevel().equals(LogLevel.DEBUG) && !cLog.isDebugEnabled())
            return;
        else if (log.getLogLevel().equals(LogLevel.TRACE) && !cLog.isTraceEnabled())
            return;

        pushLog(log,flowFile, relationship);
    }

    private void pushLog(Log log,FlowFile logFile, Relationship relationship){

        if(logFile == null)
           logFile= session.create();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("log_uuid",log.getLogUUID());
        attributes.put("log_timestamp",log.getLogTimestamp());
        attributes.put("conf_id", log.getConfId());
        attributes.put("file_uuid",log.getFileUUID());
        attributes.put("file_name",log.getFileName());
        attributes.put("log_type_id",log.getLogTypeId());
        attributes.put("log_message",log.getMessage());

        logFile = session.putAllAttributes(logFile,attributes);
        session.transfer(logFile,relationship);

        //System.out.println("LOG: "+log.getLogLevel() +": "+ log.getMessage()); // code debuging
    }

}
