package com.o2.edh.processors;

import com.o2.edh.processors.mddif.storage.sftp.MDDIFListSFTP;
import com.o2.edh.processors.mddif.storage.sftp.MDDIFListInvalidFiles;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestMDDIFListSFTP {

    final TestRunner runner = TestRunners.newTestRunner(MDDIFListSFTP.class);

    @Before
    public void setUp() {
        runner.setProperty(MDDIFListSFTP.DIRECTORY,"${directory}");
        runner.setProperty(MDDIFListSFTP.PORT,"${port}");
        runner.setProperty(MDDIFListSFTP.PRIVATE_KEY_PATH,"${private_key_path}");
        runner.setProperty(MDDIFListSFTP.USER_NAME,"${username}");
        runner.setProperty(MDDIFListSFTP.HOST_NAME,"${'hostname'}");
        runner.setProperty(MDDIFListSFTP.HOST_NAME_SEPERATOR,",");
        runner.setProperty(MDDIFListSFTP.CONF_ID,"${conf_id}");
        runner.setProperty(MDDIFListSFTP.FILE_REGEX,"${file_regex}");
    }

    @Test
    public void testListSFTP() throws IOException {

        Map<String,String> attributes = new HashMap<String,String>();
        attributes.put("directory","/home/ec2-user/data");
        attributes.put("port","22");
        attributes.put("private_key_path","C:\\Users\\yogesh.r.patel\\Downloads\\aws.pem");
        attributes.put("username","ec2-user");
        attributes.put("hostname","ec2-3-134-79-184.us-east-2.compute.amazonaws.com");
        attributes.put("conf_id","1");
        attributes.put("file_regex","(.*)");

 /*       Map<String,String> attributes1 = new HashMap<String,String>();
        attributes1.put("directory","filepath");
        attributes1.put("port","22");
        attributes1.put("private_key_path","keypath");
        attributes1.put("username","saggera");
        attributes1.put("hostname","192.168.56.102");
        attributes1.put("conf_id","2");
        attributes1.put("file_regex","^cbs_bc_customer_all_[0-9]{8}\\.verf$");*/

        runner.enqueue("hello",attributes);
        //runner.enqueue("hello",attributes1);
        runner.enqueue("hello",attributes);
        //runner.enqueue("hello",attributes1);
        runner.setThreadCount(2);
        runner.run();
    }

    @Test
    public void testValidateFileNameListSFTP() throws IOException {

        Map<String,String> attributes = new HashMap<String,String>();
        //attributes.put("directory","filepath");
        attributes.put("port","22");
        attributes.put("private_key_path","keypath");
        attributes.put("username","pately");
        attributes.put("hostname","192.168.56.102");
        attributes.put("conf_id","1");
        attributes.put("file_regex","^cbs_bc_customer_all_[0-9]{8}\\_[0-9]{1}\\_1021_[0-9]{1}\\_[0-9]{1}.unl.gz$");
        attributes.put("csvdata","/dl/dev/data/landing/nc/,^cbs_bc_customer_all_[0-9]{8}\\_[0-9]{1}\\_1021_[0-9]{1}\\_[0-9]{1}.unl.gz$,^testfile_[0-9]{1}.txt$");

        Map<String,String> attributes1 = new HashMap<String,String>();
        //attributes1.put("directory","filepath");
        attributes1.put("port","22");
        attributes1.put("private_key_path","keypath");
        attributes1.put("username","pately");
        attributes1.put("hostname","192.168.56.102");
        attributes1.put("conf_id","1");
        attributes1.put("file_regex","^cbs_bc_customer_all_[0-9]{8}\\_[0-9]{1}\\_1021_[0-9]{1}\\_[0-9]{1}.unl.gz$");
        attributes1.put("csvdata","/dl/dev/data/landing/ocs/,^cbs_bc_customer_all_[0-9]{8}\\_[0-9]{1}\\_1021_[0-9]{1}\\_[0-9]{1}.unl.gz$,^testfile_[0-9]{1}.txt$");

        Map<String,String> attributes2 = new HashMap<String,String>();
        //attributes2.put("directory","filepath");
        attributes2.put("port","22");
        attributes2.put("private_key_path","keypath");
        attributes2.put("username","saggera");
        attributes2.put("hostname","192.168.56.102");
        attributes2.put("conf_id","1");
        attributes2.put("file_regex","^cbs_bc_customer_all_[0-9]{8}\\_[0-9]{1}\\_1021_[0-9]{1}\\_[0-9]{1}.unl.gz$");
        attributes2.put("csvdata","/dl/dev/data/landing/nc/,^cbs_bc_customer_all_[0-9]{8}\\_[0-9]{1}\\_1021_[0-9]{1}\\_[0-9]{1}.unl.gz$,^testfile_[0-9]{1}.txt$");

        Map<String,String> attributes3 = new HashMap<String,String>();
        //attributes3.put("directory","filepath");
        attributes3.put("port","22");
        attributes3.put("private_key_path","keypath");
        attributes3.put("username","saggera");
        attributes3.put("hostname","hostname");
        attributes3.put("conf_id","1");
        attributes3.put("file_regex","^cbs_bc_customer_all_[0-9]{8}\\_[0-9]{1}\\_1021_[0-9]{1}\\_[0-9]{1}.unl.gz$");
        attributes3.put("csvdata","/dl/dev/data/landing/ocs/,^cbs_bc_customer_all_[0-9]{8}\\_[0-9]{1}\\_1021_[0-9]{1}\\_[0-9]{1}.unl.gz$,^testfile_[0-9]{1}.txt$");


        runner.enqueue("hello",attributes); //pately
        runner.run();
        runner.enqueue("hello",attributes2);    //saggera
        runner.run();
        runner.enqueue("hello",attributes1);    //pately
        runner.run();
        runner.enqueue("hello",attributes3);    //saggera
        runner.run();

    }

}
