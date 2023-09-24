package com.o2.edh.processors;

import com.o2.edh.processors.mddif.encryption.MDDIFEncryptContent;
import com.o2.edh.processors.mddif.encryption.CustomEncryptionMethod;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.PutFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.Security;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

public class TestMDDIFEncryptContent {

    private final TestRunner encryptRunner = TestRunners.newTestRunner(MDDIFEncryptContent.class);
    private final TestRunner decryptRunner = TestRunners.newTestRunner(MDDIFEncryptContent.class);

    private final String CONNECTION_POOL = "JDBC Connection Pool";
    private final String DATABASE = "*******";
    private final String DBPASSWORD = "*******";
    private final String DBUSERNAME = "*******";
    private final String DBHOST = "*******";

    @Before
    public void setUp() throws InitializationException {

        final DBCPService dbcp = new DBCPServiceSQLServerImpl(DATABASE,DBPASSWORD,DBUSERNAME,DBHOST);
        final Map<String, String> dbcpProperties = new HashMap<>();
        decryptRunner.addControllerService(CONNECTION_POOL, dbcp, dbcpProperties);
        decryptRunner.enableControllerService(dbcp);
        decryptRunner.assertValid(dbcp);
        decryptRunner.setProperty(MDDIFEncryptContent.CONNECTION_POOL, CONNECTION_POOL);


        Security.addProvider(new BouncyCastleProvider());
        encryptRunner.setValidateExpressionUsage(false);
        encryptRunner.setProperty(MDDIFEncryptContent.MODE, MDDIFEncryptContent.ENCRYPT_MODE);
        encryptRunner.setProperty(MDDIFEncryptContent.ENCRYPTION_ALGORITHM, CustomEncryptionMethod.PGP.name());
        encryptRunner.setProperty(MDDIFEncryptContent.PUBLIC_KEYRING, "${public_key_path}");
        encryptRunner.setProperty(MDDIFEncryptContent.PUBLIC_KEY_USERID, "${user_id}");
        encryptRunner.setProperty(MDDIFEncryptContent.SALT, "MDDIF");

        decryptRunner.setValidateExpressionUsage(false);
        decryptRunner.setProperty(MDDIFEncryptContent.MODE, MDDIFEncryptContent.DECRYPT_MODE);
        decryptRunner.setProperty(MDDIFEncryptContent.ENCRYPTION_ALGORITHM, CustomEncryptionMethod.PGP.name());
        decryptRunner.setProperty(MDDIFEncryptContent.PRIVATE_KEYRING, "${private_key_path}");
        decryptRunner.setProperty(MDDIFEncryptContent.SALT, "MDDIF");
        decryptRunner.setProperty(MDDIFEncryptContent.CONFIGURATION_TABLE, "configuration");



    }

    /**
     * Simple implementation only for MyProcessor processor testing.
     */
    private class DBCPServiceSQLServerImpl extends AbstractControllerService
            implements DBCPService {
        private static final String SQL_SERVER_CONNECT_URL = "jdbc:mysql://localhost:3306/EDH_DEV_mddif_dev1";
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
    public void testPGPDecryptSuccess() throws IOException {

        Map<String,String> attribute = new HashMap<String,String>();
        attribute.put("private_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\private_key.gpg");
        attribute.put("conf_id","1");

        decryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\output\\sample.txt.pgp"),attribute);

        decryptRunner.run();

        decryptRunner.assertTransferCount(MDDIFEncryptContent.REL_SUCCESS, 1);
        decryptRunner.assertTransferCount(MDDIFEncryptContent.LOG_RELATIONSHIP, 3);
        final MockFlowFile flowFile = decryptRunner.getFlowFilesForRelationship(MDDIFEncryptContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("Hello World!");
    }

    @Test
    public void testPGPEncryptSuccess() throws IOException {
        encryptRunner.setValidateExpressionUsage(false);
        encryptRunner.setProperty(MDDIFEncryptContent.MODE, MDDIFEncryptContent.ENCRYPT_MODE);
        Map<String,String> attribute = new HashMap<String,String>();
        attribute.put("public_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\public_key.gpg");
        attribute.put("user_id","yogesh <yogeshrpatel@yahoo.com>");

        encryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\input\\sample.txt"),attribute);

        encryptRunner.run();

        encryptRunner.assertTransferCount(MDDIFEncryptContent.REL_SUCCESS, 1);
        final MockFlowFile flowFile = encryptRunner.getFlowFilesForRelationship(MDDIFEncryptContent.REL_SUCCESS).get(0);
    }

    @Test
    public void testPGPEncryptDecryptSuccess() throws IOException {
        encryptRunner.setValidateExpressionUsage(false);
        Map<String,String> attribute = new HashMap<String,String>();
        attribute.put("public_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\Keys\\rsa_public_key.gpg");
        attribute.put("private_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\Keys\\rsa_secret_key.gpg");
        attribute.put("user_id","yogesh_dsa <yogesh.r.patel@accenture.com>");

        encryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\input\\sample.txt"),attribute);
        encryptRunner.run();
        encryptRunner.assertTransferCount(MDDIFEncryptContent.REL_SUCCESS, 1);
        final MockFlowFile flowFile = encryptRunner.getFlowFilesForRelationship(MDDIFEncryptContent.REL_SUCCESS).get(0);

        //putfile
        TestRunner putFileRunner = TestRunners.newTestRunner(PutFile.class);
        putFileRunner.enqueue(flowFile);
        putFileRunner.setProperty(PutFile.DIRECTORY,"C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\output\\");
        putFileRunner.setProperty(PutFile.CONFLICT_RESOLUTION,PutFile.REPLACE_RESOLUTION);
        putFileRunner.run();

        //decrypt
        decryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\output\\sample.txt"),attribute);
        decryptRunner.run();
        decryptRunner.assertTransferCount(MDDIFEncryptContent.REL_SUCCESS, 1);
        final MockFlowFile outflowFile = decryptRunner.getFlowFilesForRelationship(MDDIFEncryptContent.REL_SUCCESS).get(0);
        outflowFile.assertContentEquals("Hello World!");
    }

    //failure unit tests
    @Test   // private key not present
    public void testPGPDecryptNoPrivateKeyFail() throws IOException {
        Map<String,String> attribute = new HashMap<String,String>();
        attribute.put("private_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\private_key_not_present.gpg");

        decryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\output\\sample.txt.pgp"),attribute);

        decryptRunner.run();

        decryptRunner.assertTransferCount(MDDIFEncryptContent.REL_FAILURE, 1);
        decryptRunner.assertTransferCount(MDDIFEncryptContent.LOG_RELATIONSHIP, 2);
    }

    @Test   // invalid private key and password present
    public void testPGPDecryptInvalidPrivateKeyFail() throws IOException {
        Map<String,String> attribute = new HashMap<String,String>();
        attribute.put("private_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\private\\Accelerate_Huawei_Ocs_secring.gpg");

        decryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\output\\sample.txt.pgp"),attribute);

        decryptRunner.run();

        decryptRunner.assertTransferCount(MDDIFEncryptContent.REL_FAILURE, 1);
        decryptRunner.assertTransferCount(MDDIFEncryptContent.LOG_RELATIONSHIP, 2);
    }

    @Test   // public key not present
    public void testPGPEncryptNoPublicKeyFail() throws IOException {
        encryptRunner.setValidateExpressionUsage(false);
        encryptRunner.setProperty(MDDIFEncryptContent.MODE, MDDIFEncryptContent.ENCRYPT_MODE);
        Map<String,String> attribute = new HashMap<String,String>();
        attribute.put("public_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\public_key_note_present.gpg");

        encryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\input\\sample.txt"),attribute);

        encryptRunner.run();

        encryptRunner.assertTransferCount(MDDIFEncryptContent.REL_FAILURE, 1);
    }

    @Test   // invalid public key present
    public void testPGPEncryptInvalidPublicKeyFail() throws IOException {
        encryptRunner.setValidateExpressionUsage(false);
        encryptRunner.setProperty(MDDIFEncryptContent.MODE, MDDIFEncryptContent.ENCRYPT_MODE);
        Map<String,String> attribute = new HashMap<String,String>();
        attribute.put("public_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\public_key.asc");

        encryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\input\\sample.txt"),attribute);

        encryptRunner.run();

        encryptRunner.assertTransferCount(MDDIFEncryptContent.REL_FAILURE, 1);
    }

    @Test   // invalid user id present
    public void testPGPEncryptInvalidUserFail() throws IOException {
        encryptRunner.setValidateExpressionUsage(false);
        encryptRunner.setProperty(MDDIFEncryptContent.MODE, MDDIFEncryptContent.ENCRYPT_MODE);
        encryptRunner.setProperty(MDDIFEncryptContent.PUBLIC_KEY_USERID, "yogesh");
        Map<String,String> attribute = new HashMap<String,String>();
        attribute.put("public_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\public_key.gpg");

        encryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\input\\sample.txt"),attribute);

        encryptRunner.run();

        encryptRunner.assertTransferCount(MDDIFEncryptContent.REL_FAILURE, 1);
    }

    @Test
    public void testPGPDecryptNullPassowrdFail() throws IOException {

        Map<String,String> attribute = new HashMap<String,String>();
        attribute.put("private_key_path","C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\private_key.gpg");

        decryptRunner.enqueue(Paths.get("C:\\Users\\yogesh.r.patel\\Documents\\data\\key\\sample\\output\\sample.txt.pgp"),attribute);

        decryptRunner.run();

        decryptRunner.assertTransferCount(MDDIFEncryptContent.REL_FAILURE, 1);
        decryptRunner.assertTransferCount(MDDIFEncryptContent.REL_SUCCESS, 0);
        decryptRunner.assertTransferCount(MDDIFEncryptContent.LOG_RELATIONSHIP, 1);
        final MockFlowFile flowFile = decryptRunner.getFlowFilesForRelationship(MDDIFEncryptContent.REL_FAILURE).get(0);
    }


}
