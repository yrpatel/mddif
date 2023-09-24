package com.o2.edh.processors.mddif.storage.sftp;

import com.jcraft.jsch.*;
import com.o2.edh.processors.mddif.util.Log;
import com.o2.edh.processors.mddif.util.Logger;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 Created on 21-04-2020
 */
@Tags({"MDDIF","ListInvalidSFTP","SFTP","Invalid files"})
@CapabilityDescription("Processor the lists the set of files in landing directory that does not match the file regex in the configuration table")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MDDIFListInvalidFiles extends AbstractProcessor {

    public static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool")
            .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
                    + "The Connection Pool is necessary in order to determine the appropriate database column types.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table name")
            .description("Table on which the select query to be executed")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROJECT = new PropertyDescriptor.Builder()
            .name("project")
            .description("Project name")
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
        propertyDescriptors.add(TABLE_NAME);
        propertyDescriptors.add(CONNECTION_POOL);
        propertyDescriptors.add(PROJECT);
        this.descriptors = Collections.unmodifiableList(propertyDescriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(LOG_RELATIONSHIP);
        relationships.add(REL_FAILURE);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        String message = null;

        Logger logger = new Logger(session, getLogger(), LOG_RELATIONSHIP);  // [LOGGING]: Init Log
        SFTPConnectionBean.closeInactiveConnection(logger);

        String table_name = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        String project = context.getProperty(PROJECT).evaluateAttributeExpressions().getValue();

        Statement stmt1 = null;
        ResultSet rs1 = null;
        Statement stmt2 = null;
        ResultSet rs2 = null;
        FlowFile tempFlowFile = null;
        ArrayList<FlowFile> flowFileArrayList = new ArrayList<>();

        String query1 = "SELECT distinct directory, hostname, port FROM " + table_name+ " where project = '" + project+"'" ;

        ArrayList<Directory> directoryList = new ArrayList<>();

        Set<String> invalidFileList = new HashSet<>();
        final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        final Connection dbCon = dbcpService.getConnection();
        try {
            stmt1 = dbCon.createStatement();
            rs1 = stmt1.executeQuery(query1);

            while (rs1.next()) {
                 Directory directory = new Directory();
                directory.setDirectoryPath(rs1.getString(1));
                directory.setHostname(rs1.getString(2));
                directory.setPort(rs1.getString(3));
                directoryList.add(directory);
            }

            for (Directory dir : directoryList) {

                String query2 = "SELECT file_regex, username, private_key_path, quarantine_directory, support_email_list, project FROM " + table_name +
                        " WHERE directory = '" + dir.getDirectoryPath() + "'  and hostname = '" + dir.getHostname() + "' and port = '" + dir.getPort() + "' " +
                        "and project ='" +project+"'";

                stmt2 = dbCon.createStatement();
                rs2 = stmt2.executeQuery(query2);

                Set<String> fileRegexSet = new HashSet<>();
                Set<String> userNameSet = new HashSet<>();
                Set<String> privateKeyPathSet = new HashSet<>();
                Set<String> quarantineDirectorySet = new HashSet<>();
                Set<String> supportEmailListSet = new HashSet<>();
                Set<String> projectSet = new HashSet<>();

                    while (rs2.next()) {

                        addToSet(rs2.getString(1),fileRegexSet);
                        addToSet(rs2.getString(2),userNameSet);
                        addToSet(rs2.getString(3),privateKeyPathSet);
                        addToSet(rs2.getString(4),quarantineDirectorySet);
                        addAllToSet(rs2.getString(5),supportEmailListSet);
                        addToSet(rs2.getString(6),projectSet);

                    }

                    dir.setUsername(getSetValues(userNameSet, 1));
                    dir.setPrivateKeyPath(getSetValues(privateKeyPathSet, 1));
                    dir.setQuarantineDirectory(getSetValues(quarantineDirectorySet, 1));
                    dir.setSupportEmailList(getSetValues(supportEmailListSet, supportEmailListSet.size()));
                    dir.setProject(getSetValues(projectSet, 1)); //to be tested

                String[] hostList = dir.getHostname().split(",");
                String ipAddress = "";

                try {
                    ipAddress = InetAddress.getLocalHost().getHostAddress();
                    ipAddress = ipAddress.substring(ipAddress.lastIndexOf('.') + 1);
                } catch (UnknownHostException e) {
                    logger.generateLog(new Log(LogLevel.ERROR, null, null, null, "331003",
                            "Hostname not reachable/unknown " + e.getMessage()));
                    return;
                }

                for (String hostName : hostList) {
                    hostName = hostName.trim();
                    logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"131001",
                            "Connecting to Host: " + hostName));

                    try {
                        invalidFileList = getSFTPInvalidFileList(hostName, dir.getPort(), dir.getDirectoryPath(), fileRegexSet
                                ,dir.getUsername(),dir.getPrivateKeyPath(),logger);

                        for (String str : invalidFileList) {

                            String uuid = UUID.randomUUID().toString();
                            tempFlowFile = session.create();

                            tempFlowFile = session.putAttribute(tempFlowFile, "file_uuid", ipAddress + "-" + uuid);
                            tempFlowFile = session.putAttribute(tempFlowFile, "localhost", hostName);
                            tempFlowFile = session.putAttribute(tempFlowFile, "file_name", str);
                            tempFlowFile = session.putAttribute(tempFlowFile, "support_email_list", dir.getSupportEmailList());
                            tempFlowFile = session.putAttribute(tempFlowFile, "quarantine_directory",dir.getQuarantineDirectory());
                            tempFlowFile = session.putAttribute(tempFlowFile, "project",dir.getProject());
                            tempFlowFile = session.putAttribute(tempFlowFile, "directory",dir.getDirectoryPath());
                            tempFlowFile = session.putAttribute(tempFlowFile, "port",dir.getPort());
                            tempFlowFile = session.putAttribute(tempFlowFile, "username",dir.getUsername());
                            tempFlowFile = session.putAttribute(tempFlowFile, "private_key_path",dir.getPrivateKeyPath());

                            flowFileArrayList.add(tempFlowFile);
                            //flowFileArrayList is sent to SUCCESS
                        }
                    } catch (JSchException | SftpException e ) {
                        String uuid = UUID.randomUUID().toString();
                        tempFlowFile = session.create();
                        tempFlowFile = session.putAttribute(tempFlowFile, "file_uuid", ipAddress + "-" + uuid);
                        tempFlowFile = session.putAttribute(tempFlowFile, "localhost", hostName);
                        tempFlowFile = session.putAttribute(tempFlowFile, "file_name", "null");
                        tempFlowFile = session.putAttribute(tempFlowFile, "support_email_list", dir.getSupportEmailList());
                        tempFlowFile = session.putAttribute(tempFlowFile, "quarantine_directory",dir.getQuarantineDirectory());
                        tempFlowFile = session.putAttribute(tempFlowFile, "project",dir.getProject());
                        tempFlowFile = session.putAttribute(tempFlowFile, "directory",dir.getDirectoryPath());
                        tempFlowFile = session.putAttribute(tempFlowFile, "port",dir.getPort());
                        tempFlowFile = session.putAttribute(tempFlowFile, "username",dir.getUsername());
                        tempFlowFile = session.putAttribute(tempFlowFile, "private_key_path",dir.getPrivateKeyPath());
                        logger.generateLog(new Log(LogLevel.ERROR, null, null, null, "331004",
                                "SFTP Caught Exception : " + e.getMessage()),tempFlowFile,REL_FAILURE);

                    }

                    //to handle null values for file_regex
                    catch(NullPointerException e) {
                        message = "NullPointerException " + e.getMessage();
                        logger.generateLog(new Log(LogLevel.ERROR, null, null, null,"331005", message),
                                null,REL_FAILURE);
                    }
                }

            }

            logger.generateLog(new Log(LogLevel.INFO, null, null, null,"231001",
                    "Successfully listed invalid files from the configuration table"),null, LOG_RELATIONSHIP);
            session.transfer(flowFileArrayList, REL_SUCCESS);

        } catch (SQLException e) {
            message = "Failed to execute query - " + e.getMessage();
            logger.generateLog(new Log(LogLevel.ERROR, null, null, null, "331001", message));
        }
        finally {
            try {
                dbCon.close();
            } catch (SQLException e) {
                message = "Failed to close SQL connection - " + e.getMessage();
                logger.generateLog(new Log(LogLevel.ERROR, null, null, null, "331002", message));
                session.rollback();
                context.yield();
            }
        }

    }   //onTrigger

    public Set<String> getSFTPInvalidFileList(String hostName, String port, String directory, Set<String> fileRegexSet,
                                              String username,String privateKeyPath, Logger logger)
            throws JSchException, SftpException {

        Session session = SFTPConnectionBean.getSession(hostName,port,username, privateKeyPath, logger);
        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"131002",
                "Connected to sftp://"+ hostName +":"+ port + directory + "/"));


        Set<String> validFiles = new HashSet<String>();
        Set<String> invalidFiles = new HashSet<String>();

        Matcher matcher = null;

        @SuppressWarnings("unchecked")
        Vector<ChannelSftp.LsEntry> directoryEntries = sftpChannel.ls(String.valueOf(directory));
        for (ChannelSftp.LsEntry file : directoryEntries) {
            if (!file.getFilename().equals(".") && !file.getFilename().equals("..")) {
                for (String cmp : fileRegexSet) {
                    Pattern p = Pattern.compile(cmp);
                    matcher = p.matcher(file.getFilename());
                    if (matcher.matches()) {
                        validFiles.add(file.getFilename());
                    } else {
                        invalidFiles.add(file.getFilename());
                    }
                    invalidFiles.removeAll(validFiles);
                }
            }
        }

        channel.disconnect();
        return invalidFiles;
    }

    public String getSetValues(Set<String> set, int index) {
        Iterator<String> itr = set.iterator();
        String returnval = "";
        int i=0;
        while(itr.hasNext()){
            String val = itr.next();
            if(i < index)
                returnval = returnval +val+ ",";
            i++;
        }
        if(returnval.length() ==0)
            return returnval;
        return returnval.substring(0,returnval.length()-1);
    }

    private void addToSet(String rs, Set<String> set ) {
             if (rs != null)
                set.add(rs);
    }

    private void addAllToSet(String data, Set<String> set) {
        if(data!= null)
            set.addAll(Arrays.asList(data.split(",")));
    }
}
