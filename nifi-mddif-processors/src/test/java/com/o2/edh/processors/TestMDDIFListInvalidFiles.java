package com.o2.edh.processors;

import com.o2.edh.processors.TestMDDIFEncryptContent;
import com.o2.edh.processors.mddif.encryption.MDDIFEncryptContent;
import com.o2.edh.processors.mddif.storage.sftp.MDDIFListInvalidFiles;
import com.o2.edh.processors.mddif.validators.MDDIFManifestValidator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

public class TestMDDIFListInvalidFiles {

    private final String CONNECTION_POOL = "JDBC Connection Pool";
    private final String DATABASE = "*******";
    private final String DBPASSWORD = "*******";
    private final String DBUSERNAME = "*******";
    private final String DBHOST = "*******";

    private final TestRunner runner = TestRunners.newTestRunner(MDDIFListInvalidFiles.class);

    @Before
    public void setUp() throws InitializationException {

        final DBCPService dbcp = new DBCPServiceSQLServerImpl(DATABASE, DBPASSWORD, DBUSERNAME, DBHOST);
        final Map<String, String> dbcpProperties = new HashMap<>();
        runner.addControllerService(CONNECTION_POOL, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.assertValid(dbcp);
        runner.setProperty(MDDIFListInvalidFiles.CONNECTION_POOL, CONNECTION_POOL);
        runner.setProperty(MDDIFListInvalidFiles.TABLE_NAME, "configuration_v5");
        runner.setProperty(MDDIFListInvalidFiles.PROJECT, "Accelerate");
    }

    /**
     * Simple implementation only for MyProcessor processor testing.
     */
    private class DBCPServiceSQLServerImpl extends AbstractControllerService
            implements DBCPService {
        private static final String SQL_SERVER_CONNECT_URL = "jdbc:mysql://localhost:3306/edh_dev_mddif_dev1";
        private String containerDB;
        private String password;
        private String userName;
        private String dbHost;
        public DBCPServiceSQLServerImpl(String containerDB, String password,
                                        String userName, String dbHost) {
            super();
            this.containerDB = containerDB;
            this.password = password;
            this.userName = userName;
            this.dbHost = dbHost;
        }
        @Override
        public String getIdentifier() {
            return CONNECTION_POOL;
        }
        @Override
        public Connection getConnection() throws ProcessException {
            try {
               Class.forName("com.mysql.cj.jdbc.Driver");
            //    Class.forName("com.mysql.jdbc.Driver");
                Connection connection = DriverManager.getConnection(String
                                .format(SQL_SERVER_CONNECT_URL, dbHost, containerDB),
                        userName, password);
                return connection;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    @Test
    public void testInvalidFiles(){
        int success_count =0;
        runner.setValidateExpressionUsage(false);
        runner.run();

        runner.assertTransferCount(MDDIFListInvalidFiles.REL_SUCCESS, success_count);

        for(int i=0;i<success_count;i++) {
            final MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(MDDIFListInvalidFiles.REL_SUCCESS).get(i);
            System.out.println("\n SUCCESS : Flowfile Attributes of file :" +i);
            Map<String, String> writeAttributes = successFlowFile.getAttributes();
            for (Map.Entry<String, String> entry : writeAttributes.entrySet())
                System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
        int failure_count = 1;
        runner.assertTransferCount(MDDIFListInvalidFiles.REL_FAILURE, failure_count);
        for(int i=0;i<failure_count;i++) {
            final MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(MDDIFListInvalidFiles.REL_SUCCESS).get(i);
            System.out.println("\n FAILURE : Flowfile Attributes of file :" + i);
            Map<String, String> writeAttributes = successFlowFile.getAttributes();
            for (Map.Entry<String, String> entry : writeAttributes.entrySet())
                System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }

        runner.assertTransferCount(MDDIFListInvalidFiles.LOG_RELATIONSHIP, 2);
        for(int i=0;i<2;i++) {
            final MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(MDDIFListInvalidFiles.LOG_RELATIONSHIP).get(i);
            System.out.println("\n LOGS : Flowfile Attributes of file :" + i);
            Map<String, String> writeAttributes = successFlowFile.getAttributes();
            for (Map.Entry<String, String> entry : writeAttributes.entrySet())
                System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }




    }
}
