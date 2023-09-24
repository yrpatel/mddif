package com.o2.edh.processors.mddif.partition;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.o2.edh.processors.mddif.util.Log;
import com.o2.edh.processors.mddif.util.Logger;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.text.SimpleDateFormat;
import java.util.*;

@Tags({"Partition Directory", "Hive Partition"})
@CapabilityDescription("This processor generates the Partitions dynamically")

public class MDDIFPartitionBuilder extends AbstractProcessor {

    public static final PropertyDescriptor PARTITION_FORMAT = new PropertyDescriptor
            .Builder().name("Partition Format")
            .displayName("Partition Format")
            .description("Partition Format is a JSON input")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed")
            .build();
    public static final Relationship LOG_RELATIONSHIP = new Relationship.Builder()
            .name("log")
            .description("Log Relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PARTITION_FORMAT);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(LOG_RELATIONSHIP);
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile inFlowFile = session.get();
        if (inFlowFile == null) {
            return;
        }

        //get flowfile attributes
        String partitionFormatText = context.getProperty(PARTITION_FORMAT).evaluateAttributeExpressions(inFlowFile).getValue();
        String filename = inFlowFile.getAttribute("file_name");
        String confId = inFlowFile.getAttribute("conf_id");
        String fileUUID = inFlowFile.getAttribute("file_uuid");

        // [LOGGING]: Init Log
        Logger logger = new Logger(session,getLogger(),LOG_RELATIONSHIP);
        logger.generateLog(new Log(LogLevel.DEBUG,confId,fileUUID,filename,"126001","Logger initiated"));
        boolean error = false;
        Partition partitions = null;
        String rawPartition ="";
        String hivePartition ="";

        if(partitionFormatText.isEmpty()){
            inFlowFile = session.putAttribute(inFlowFile,"raw_partition", rawPartition);
            inFlowFile = session.putAttribute(inFlowFile,"hive_partition", hivePartition);

            logger.generateLog(new Log(LogLevel.INFO, confId, fileUUID, filename, "226005",
                    "Successfully generated partition attributes."));
            session.transfer(inFlowFile,REL_SUCCESS);
            return;
        }

        try {
            Gson gson = new Gson();
            partitions = gson.fromJson(partitionFormatText, Partition.class);
            logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUUID, filename, "126002", //change this number
                    "Successfully Parsed Partition Format JSON: " + partitions.toString()));
        } catch(JsonSyntaxException e) {
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326006", //change this number
                    "ERROR while parsing Partition Format JSON" +e.getClass().getName()+ e.getMessage()),inFlowFile, REL_FAILURE);
            return;
        }

        Date date = new Date();
        for(int i=0; i< partitions.getpartitionArr().length; i++){
            String columnName = partitions.getpartitionArr()[i].getColumnName();
            String format = partitions.getpartitionArr()[i].getFormat();
            String source = partitions.getpartitionArr()[i].getSource();

            if(rawPartition.length() != 0){ // this is for Hierarchy
                rawPartition += "/";
                hivePartition += ",";
            }

            final String rawColumnName = (columnName==null || columnName.isEmpty()) ? "" : (columnName + "=");
            switch (source.toLowerCase()){
                case "date":
                    if(format==null || format.length()==0){
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326007", //change this number
                                "ERROR partition source is set to date but format is not set in JSON"),inFlowFile, REL_FAILURE);
                        return;
                    }
                    SimpleDateFormat df = null;
                    try{
                        df = new SimpleDateFormat(format);
                        df.format(date);
                    }catch (IllegalArgumentException e){
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326008", //change this number
                                        "ERROR partition source is set to date but with invalid format is provided in JSON." +
                                                " Provided value: " + format),
                                inFlowFile, REL_FAILURE);
                        return;
                    }
                    rawPartition += rawColumnName +df.format(date);
                    hivePartition += df.format(date);
                    break;
                case "datewithmod":
                    if(format!=null && format.length()!=0){
                        SimpleDateFormat dfMain = null;
                        try{
                            dfMain = new SimpleDateFormat(format);
                            dfMain.format(date);
                        }catch (IllegalArgumentException e){
                            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326009", //change this number
                                            "ERROR partition source is set to date but with invalid format is provided in JSON." +
                                                    " Provided value: " + format),
                                    inFlowFile, REL_FAILURE);
                            return;
                        }
                        rawPartition += rawColumnName +dfMain.format(date);
                        hivePartition += dfMain.format(date);
                    }

                    String appendMod = partitions.getpartitionArr()[i].getAppendMod();
                    if(appendMod==null || appendMod.length() < 4){
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326010", //change this number
                                "ERROR partition source is set to dateWithMod but appendMod is not set correctly in JSON. " +
                                        "appendMod should have first 2 chars of (modFormat) date or time pattern; " +
                                        "and last 2 chars with modByValue"),inFlowFile, REL_FAILURE);
                        return;
                    }
                    String modFormat = appendMod.substring(0,2);
                    int modValue = 0;
                    int modBy = 0;
                    try {
                        SimpleDateFormat dfMod = new SimpleDateFormat(modFormat);
                        modValue = Integer.parseInt(dfMod.format(date)); //current time
                        modBy = Integer.parseInt(appendMod.substring(2, 4)); //15
                    }catch (NumberFormatException e){
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326011", //change this number
                                        "ERROR partition source is set to dateWithMod but with invalid modBy or modByValue in JSON." +
                                                " if modFormat is HH then modByValue cannot be more than 24" +
                                                " Provided value: " + modFormat),
                                inFlowFile, REL_FAILURE);
                        return;
                    }catch (IllegalArgumentException e){
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326012", //change this number
                                        "ERROR partition source is set to dateWithMod but with invalid modFormat in appendMod in JSON." +
                                                " Support modFormat ss, mm, HH." +
                                                " Provided value: " + modFormat),
                                inFlowFile, REL_FAILURE);
                        return;
                    }

                    int modIntResult =0;
                    int modDivideValue = 0;

                    switch (modFormat){
                        case "HH":
                            if(modBy > 24){ // there are only 24 hours
                                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326013", //change this number
                                                "ERROR partition source is set to dateWithMod but with invalid modByValue in appendMod in JSON." +
                                                        " if modFormat is HH then modByValue cannot be more than 24"),
                                        inFlowFile, REL_FAILURE);
                                return;
                            }
                        case "ss":
                        case "mm":
                            if(modBy > 60){ // there are only 60 seconds or minutes
                                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326014", //change this number
                                                "ERROR partition source is set to dateWithMod but with invalid modByValue in appendMod in JSON." +
                                                        " modFormat ss or mm then modByValue cannot be more than 60"),
                                        inFlowFile, REL_FAILURE);
                                return;
                            }
                            modDivideValue = Integer.parseInt(String.valueOf((int) modValue/modBy)); // 0 ~ 0.4 = 15/15 current  mins
                            modIntResult = modDivideValue * modBy;
                            break;
                        default:
                            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326015", //change this number
                                    "ERROR partition source is set to dateWithMod but with invalid modFormat in appendMod in JSON." +
                                            " Support modFormat ss, mm, HH"),
                                    inFlowFile, REL_FAILURE);
                            return;
                    }
                    String modResult = modIntResult < 10 ? "0"+modIntResult : ""+modIntResult;

                    logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUUID, filename, "126003", //change this number
                            "FINDINGS for datewithmod: "
                                    + "\nmodFormat: " + modFormat
                                    + "\nmodValue: "+ modValue
                                    + "\nmodBy: "+modBy
                                    + "\nmodByMinus1"
                                    + "\nmodDivideValue: " + modDivideValue
                                    + "\nmodIntResult: " + modIntResult
                                    + "\nmodResult: " + modResult));

                    rawPartition += modResult;
                    hivePartition += modResult;
                    break;
                case "attribute":
                    String attributeName = partitions.getpartitionArr()[i].getAttributeName();
                    if(attributeName==null || attributeName.length()==0){
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326016", //change this number
                                "ERROR partition source is set to attribute but attributeName is not set in JSON"),inFlowFile, REL_FAILURE);
                        return;
                    }
                    String attributeValue = inFlowFile.getAttribute(attributeName);
                    if(attributeValue == null || attributeValue.length()==0){
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"326017", //change this number
                                "ERROR partition source is set to attribute '"+ attributeName+ "' but not provided with flowfile.")
                                , inFlowFile, REL_FAILURE);
                        return;
                    }
                    rawPartition += attributeName+"="+attributeValue;
                    hivePartition += attributeValue;
                    break;
                default:
                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename, "326018", // change this number
                            "Invalid partition column source."),inFlowFile,REL_FAILURE);
            }
        }

        inFlowFile = session.putAttribute(inFlowFile,"raw_partition", rawPartition);
        inFlowFile = session.putAttribute(inFlowFile,"hive_partition", hivePartition);

        logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUUID, filename, "126004", //change this number
                "Successfully generated partition attributes: \t raw_partition: " + rawPartition +
                        "\t hive_partition: " + hivePartition +
                        "\n "));
        if(error){
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename, "326019",  //change this number
                    "Error occurred Transferring to failure"),inFlowFile,REL_FAILURE);
        } else {
          //  inFlowFile = session.putAttribute(inFlowFile, "filename", filename.split("\\.")[0] + ".csv");
            logger.generateLog(new Log(LogLevel.INFO, confId, fileUUID, filename, "226005",
                    "Successfully generated partition attributes."));
            session.transfer(inFlowFile,REL_SUCCESS);
        }

    }
}
