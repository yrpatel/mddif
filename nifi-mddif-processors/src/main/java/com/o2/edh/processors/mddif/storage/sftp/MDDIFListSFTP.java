package com.o2.edh.processors.mddif.storage.sftp;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.jcraft.jsch.*;
import com.o2.edh.processors.mddif.util.Log;
import com.o2.edh.processors.mddif.util.Logger;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by aditya.thakare on 11/18/2018.
 */
// Note that we do not use @SupportsBatching annotation. This processor cannot support batching because it must ensure that session commits happen before remote files are deleted.
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven //cannot make this event driven as we have to closed the un used sessions.
//TriggerSerially our code should be thread safe
@Tags({"MDDIF","ListSFTP","SFTP"})
@CapabilityDescription("MDDIF ListSFTP Processor")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MDDIFListSFTP extends AbstractProcessor {

    public static final PropertyDescriptor HOST_NAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("Hostname, can be multiple values")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor HOST_NAME_SEPERATOR = new PropertyDescriptor.Builder()
            .name("Hostname Separator")
            .description("Hostname Separator, can a single character")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("Port")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PRIVATE_KEY_PATH = new PropertyDescriptor.Builder()
            .name("private_key_path")
            .description("private_key_path")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USER_NAME = new PropertyDescriptor.Builder()
            .name("UserName")
            .description("UserName")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("Directory")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor FILE_REGEX = new PropertyDescriptor.Builder()
            .name("File Regex")
            .description("File Regex")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONF_ID = new PropertyDescriptor.Builder()
            .name("Configuration ID")
            .description("Configuration ID")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
            .build();
    public static final Relationship LOG_RELATIONSHIP = new Relationship.Builder()
            .name("log")
            .description("Log relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        this.descriptors = Collections.unmodifiableList(descriptors);
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<PropertyDescriptor>();
        propertyDescriptors.add(HOST_NAME);
        propertyDescriptors.add(HOST_NAME_SEPERATOR);
        propertyDescriptors.add(PORT);
        propertyDescriptors.add(USER_NAME);
        propertyDescriptors.add(PRIVATE_KEY_PATH);
        propertyDescriptors.add(DIRECTORY);
        propertyDescriptors.add(FILE_REGEX);
        propertyDescriptors.add(CONF_ID);
        this.descriptors = Collections.unmodifiableList(propertyDescriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(LOG_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnStopped
    public void onStopped(){
        System.out.println("inside onStopped");
        try {
            SFTPConnectionBean.closeAllSessions(getLogger());
        } catch (JSchException e) {
            //e.printStackTrace();
            getLogger().log(LogLevel.ERROR,e.getClass().getName() + e.getMessage());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException { //onTrigger is call every scheduled time/event.

        Logger logger = new Logger(session,getLogger(),LOG_RELATIONSHIP);  // [LOGGING]: Init Log
        SFTPConnectionBean.closeInactiveConnection(logger);
        FlowFile inFlowFile = session.get();

        if ( inFlowFile == null ) {
            return;
        }

        logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"110018", //[LOGGING]: Sample Log
                "Logger initiated"));

        String hostNameStr = context.getProperty(HOST_NAME).evaluateAttributeExpressions(inFlowFile).getValue();
        String hostNameSeparator = context.getProperty(HOST_NAME_SEPERATOR).evaluateAttributeExpressions(inFlowFile).getValue();
        String port = context.getProperty(PORT).evaluateAttributeExpressions(inFlowFile).getValue();
        String userName = context.getProperty(USER_NAME).evaluateAttributeExpressions(inFlowFile).getValue();
        String privateKey = context.getProperty(PRIVATE_KEY_PATH).evaluateAttributeExpressions(inFlowFile).getValue();
        String confId = context.getProperty(CONF_ID).evaluateAttributeExpressions(inFlowFile).getValue();
        String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions(inFlowFile).getValue();
        String fileRegex = context.getProperty(FILE_REGEX).evaluateAttributeExpressions(inFlowFile).getValue();

        if(hostNameSeparator.length() !=1){
            logger.generateLog(new Log(LogLevel.ERROR,confId,null,null,"310024",
                    "Invalid Host Separator"),inFlowFile,REL_FAILURE);
            return;
        }
        String[] hostList = hostNameStr.split(hostNameSeparator);

        logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"110019",
                "Received Host List : " + hostNameStr));

        //check host and return if fail
        if(hostList.length==0){
            logger.generateLog(new Log(LogLevel.ERROR,confId,null,null,"310025",
                    "No hostname Found"),inFlowFile,REL_FAILURE);
            return;
        }

        String ipAddress;
        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
            ipAddress = ipAddress.substring(ipAddress.lastIndexOf('.') + 1);
        } catch (UnknownHostException e) {
            logger.generateLog(new Log(LogLevel.ERROR,confId,null,null,"310026",
                    "NiFi Host not reachable/unknown " + e.getMessage()), inFlowFile, REL_FAILURE);
            return;
        }

        FlowFile  outFlowFile;
        for (String hostName : hostList) {
            hostName = hostName.trim(); // trimming for space issues
            logger.generateLog(new Log(LogLevel.DEBUG,confId,null,null,"110020",
                    "Connecting to Host: " + hostName));

            outFlowFile = session.create(inFlowFile);

            JsonArray jsonFileList = new JsonArray();
            List<File> fileList;
            try {
                fileList = getSFTPFileList(hostName, port, userName, privateKey,confId, directory, fileRegex, logger);
            } catch (JSchException | SftpException e) {
                e.printStackTrace();
                logger.generateLog(new Log(LogLevel.ERROR,confId,null,null,"310027",
                        "For host: "+ hostName + "; SFTP Caught Exception: "
                                + e.getClass().getName()+  e.getMessage()), outFlowFile, REL_FAILURE);
                continue;
            } catch (PatternSyntaxException e) {
                e.printStackTrace();
                logger.generateLog(new Log(LogLevel.ERROR,confId,null,null,"310029",
                        "For host: "+ hostName + "Invalid file regex pattern : "
                                + e.getClass().getName()+  e.getMessage()), outFlowFile, REL_FAILURE);
                continue;
            }

            for (File listedFile : fileList) {
                JsonObject jsonFileDetail = new JsonObject();
                jsonFileDetail.addProperty("listing_timestamp", listedFile.getLastModifiedDatetime());
                String uuid = UUID.randomUUID().toString();

                jsonFileDetail.addProperty("file_uuid", ipAddress + "-" + uuid);
                jsonFileDetail.addProperty("conf_id", confId);
                jsonFileDetail.addProperty("file_name", listedFile.getFileName());
                jsonFileDetail.addProperty("directory", directory);
                jsonFileDetail.addProperty("actual_file_size", listedFile.getFileSize());
                jsonFileDetail.addProperty("localhost", hostName);
                jsonFileList.add(jsonFileDetail);
            }
            outFlowFile = session.write(outFlowFile, outputStream -> outputStream.write(jsonFileList.toString().getBytes()));
            session.transfer(outFlowFile, REL_SUCCESS);
            logger.generateLog(new Log(LogLevel.INFO,confId,null,null,"210023",
                    "Successfully listed files on node:" + hostName));
        }
        //Purge inFlowFile.
        session.remove(inFlowFile);
    }

    private List<File> getSFTPFileList(String hostName, String port, String userName,
                                       String privateKeyPath, String confId, String directory, String fileRegex, Logger logger)
            throws JSchException, SftpException, PatternSyntaxException {

        Session session = SFTPConnectionBean.getSession(hostName,port,userName, privateKeyPath, logger);
        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        logger.generateLog(new Log(LogLevel.DEBUG,confId,null,null,"110021",
                "Connected to sftp://"+ hostName +":"+ port + directory + "/" +fileRegex));


        @SuppressWarnings("unchecked")
        Vector<ChannelSftp.LsEntry> directoryEntries = sftpChannel.ls(directory);

        List<File> fileList = new ArrayList<>();
        //file regex match
        Pattern pattern = Pattern.compile(fileRegex);
        Matcher matcher;

        for (ChannelSftp.LsEntry file : directoryEntries) {
            matcher = pattern.matcher(file.getFilename());
            if(matcher.matches()){
                File listedFileInfo = new File();
                listedFileInfo.setFileName(file.getFilename());
                listedFileInfo.setFileSize(file.getAttrs().getSize());
                listedFileInfo.setLastModifiedDatetime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        .format(new Date(file.getAttrs().getMTime() * 1000L)));
                fileList.add(listedFileInfo);
            }
            logger.generateLog(new Log(LogLevel.DEBUG,confId,null,null,"110022",
                    "Found File: sftp://"+ hostName +":"+ port + directory + "/"
                            + file.getFilename()));
        }

        channel.disconnect();
        return fileList;
    }
}