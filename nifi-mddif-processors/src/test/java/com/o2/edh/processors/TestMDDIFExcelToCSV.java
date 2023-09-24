package com.o2.edh.processors;

import com.o2.edh.processors.mddif.fileconversion.MDDIFExcelToCSV;
import org.apache.nifi.processors.standard.PutFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestMDDIFExcelToCSV {

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(MDDIFExcelToCSV.class);
        runner.setProperty(MDDIFExcelToCSV.FIELD_SEPERATOR,"${field_separator}");
        runner.setProperty(MDDIFExcelToCSV.DATE_FORMAT,"${date_format}");
    }

    @Test
    public void testRunFileConversion() throws IOException {
        Map<String, String> attributes = new HashMap();
        attributes.put("field_separator","|");
        attributes.put("conf_id","1");
        attributes.put("file_uuid","1234");
        attributes.put("filename","MDDIFExcelToCSV.csv");
        attributes.put("date_format","MM/dd/yyyy");

        runner.enqueue(Paths.get("src\\test\\files\\input_excel_file.xls"),attributes);
        runner.run();
        MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(MDDIFExcelToCSV.REL_SUCCESS).get(0);


        //putfile
        TestRunner putFileRunner = TestRunners.newTestRunner(PutFile.class);
        putFileRunner.enqueue(successFlowFile);
        putFileRunner.setProperty(PutFile.DIRECTORY,"src\\test\\files\\output\\");
        putFileRunner.setProperty(PutFile.CONFLICT_RESOLUTION,PutFile.REPLACE_RESOLUTION);
        putFileRunner.run();

        Map<String, String> writeAttributes = successFlowFile.getAttributes();
        for (Map.Entry<String,String> entry : writeAttributes.entrySet())
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());


    }
}
