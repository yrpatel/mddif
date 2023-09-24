package com.o2.edh.processors.mddif.validators.helpers;

import com.o2.edh.processors.mddif.util.Log;
import com.o2.edh.processors.mddif.util.Logger;
import org.apache.avro.Schema;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Set;

public class MetadataFields {
    private String file_name;
    private Long file_size;
    private Long record_count;
    private Long num_of_fields;
    private String data_end_date;
    private String date_of_file_generation;
    private String data_start_date;
    private Long sequenceNumber;

    public String getFile_name() {
        return file_name;
    }

    public void setFile_name(String file_name) {
        this.file_name = file_name;
    }

    public Long getFile_size() {
        return file_size;
    }

    public void setFile_size(Long file_size) {
        this.file_size = file_size;
    }

    public Long getRecord_count() {
        return record_count;
    }

    public void setRecord_count(Long record_count) {
        this.record_count = record_count;
    }

    public Long getNum_of_fields() {
        return num_of_fields;
    }

    public void setNum_of_fields(Long num_of_fields) {
        this.num_of_fields = num_of_fields;
    }

    public String getData_end_date()
    {
        return data_end_date;
    }

    public void setData_end_date(String data_end_date)
    {
        this.data_end_date = data_end_date;
    }

    public String getDate_of_file_generation()
    {
        return date_of_file_generation;
    }

    public void setDate_of_file_generation(String date_of_file_generation) {
        this.date_of_file_generation = date_of_file_generation;
    }

    public String getData_start_date() {
        return data_start_date;
    }

    public void setData_start_date(String data_start_date) {
        this.data_start_date = data_start_date;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(Long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public static MetadataFields getManifestFields(Record line, RecordSchema metadataSchema,
                                                   String confId, String fileUuid, String fileName, Logger logger)
            throws NumberFormatException, ParseException {

        MetadataFields manifestFieldObj = new MetadataFields();
        String fieldName;

        Schema.Parser parser = new Schema.Parser();
        String metaSchemaText = metadataSchema.toString();
        Schema metadataAvroSchema = parser.parse(metaSchemaText);

        String sourceDateFormat = null;
        Set<String> fieldNames = line.getRawFieldNames();
        Iterator<String> itr = fieldNames.iterator();
        while (itr.hasNext()) {
            fieldName = itr.next();
            switch (fieldName.toLowerCase()){
                case "file_name" :
                    manifestFieldObj.setFile_name(line.getAsString(fieldName));
                    break;
                case "sequence_number" :
                    manifestFieldObj.setSequenceNumber(line.getAsLong(fieldName));
                    break;
                case "file_size":
                    manifestFieldObj.setFile_size(line.getAsLong(fieldName));
                    break;
                case "record_count":
                    manifestFieldObj.setRecord_count(line.getAsLong(fieldName));
                    break;
                case "num_of_fields":
                    manifestFieldObj.setNum_of_fields(line.getAsLong(fieldName));
                    break;
                case "data_start_date":
                    sourceDateFormat = metadataAvroSchema.getField(fieldName).doc();
                    if(sourceDateFormat==null){
                        manifestFieldObj.setData_start_date(line.getAsString(fieldName));
                            break;
                    }else if(sourceDateFormat.equals("")){
                        manifestFieldObj.setData_start_date(line.getAsString(fieldName));
                        break;
                    } else { //empty
                        String targetDateFormat = "yyyyMMddHHmmss";
                        SimpleDateFormat sourceDf = new SimpleDateFormat(sourceDateFormat);
                        SimpleDateFormat targetDf = new SimpleDateFormat(targetDateFormat);
                        manifestFieldObj.setData_start_date(targetDf.format(sourceDf.parse(String.valueOf(line.getAsString(fieldName)))));
                        break;
                    }
                case "data_end_date":
                    sourceDateFormat = metadataAvroSchema.getField(fieldName).doc();
                    if(sourceDateFormat==null){
                        manifestFieldObj.setData_end_date(line.getAsString(fieldName));
                        break;
                    }else if(sourceDateFormat.equals("")){
                        manifestFieldObj.setData_end_date(line.getAsString(fieldName));
                        break;
                    } else { //empty
                        String targetDateFormat = "yyyyMMddHHmmss";
                        SimpleDateFormat sourceDf = new SimpleDateFormat(sourceDateFormat);
                        SimpleDateFormat targetDf = new SimpleDateFormat(targetDateFormat);
                        manifestFieldObj.setData_end_date(targetDf.format(sourceDf.parse(String.valueOf(line.getAsString(fieldName)))));
                        break;
                    }
                case "date_of_file_generation":
                    sourceDateFormat = metadataAvroSchema.getField(fieldName).doc();
                    if(sourceDateFormat==null){
                        manifestFieldObj.setDate_of_file_generation(line.getAsString(fieldName));
                        break;
                    }else if(sourceDateFormat.equals("")){
                        manifestFieldObj.setDate_of_file_generation(line.getAsString(fieldName));
                        break;
                    } else { //empty
                        String targetDateFormat = "yyyyMMddHHmmss";
                        SimpleDateFormat sourceDf = new SimpleDateFormat(sourceDateFormat);
                        SimpleDateFormat targetDf = new SimpleDateFormat(targetDateFormat);
                        manifestFieldObj.setDate_of_file_generation(targetDf.format(sourceDf.parse(String.valueOf(line.getAsString(fieldName)))));
                        break;
                    }
                default:
                    logger.generateLog(new Log(LogLevel.DEBUG,confId,fileUuid,fileName,"125000",
                            "Field didn't match: " + line.getAsString(fieldName)));
            }
        }
        return manifestFieldObj;
    }

}
