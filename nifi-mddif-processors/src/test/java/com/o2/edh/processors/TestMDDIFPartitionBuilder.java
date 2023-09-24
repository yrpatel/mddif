package com.o2.edh.processors;

import com.o2.edh.processors.mddif.partition.MDDIFPartitionBuilder;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestMDDIFPartitionBuilder {

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(MDDIFPartitionBuilder.class);
        runner.setProperty(MDDIFPartitionBuilder.PARTITION_FORMAT, "${partition_format}");
        //{"partitionArr":[{"columnName":"load_date", "source":"dateWithMod", "format":"yyyyMMddHH", "appendMod":"mm15"}]}
    }

    @Test
    public void testAllPartition() throws IOException {
        HashMap<String, String> attributes = new HashMap();
        //attributes.put("field_separator","|");
        attributes.put("conf_id","1");
        attributes.put("file_uuid","1234");
        attributes.put("filename","test.xls");
        attributes.put("sequence_id","12345");
        //attributes.put("partition_format","{\"partitionArr\":[{\"columnName\":\"load_date\", \"source\":\"dateWithMod\", \"format\":\"yyyyMMddHH\", \"appendMod\":\"mm15\"} ]}");
        String val = "{\"partitionArr\":[{\"source\":\"date\", \"format\":\"yyyyMMdd\"}]}";
        String val1 = "{\"partitionArr\":[{ \"source\":\"date\", \"format\":\"yyyy\"},{ \"source\":\"date\", \"format\":\"MM\"}, {\"columnName\":\"sequence_id\", \"source\":\"attribute\", \"attributeName\":\"sequence_id\"},{\"columnName\":\"load_date\", \"source\":\"dateWithMod\", \"format\":\"yyyyMMddHH\", \"appendMod\":\"mm15\"} ]}";
        attributes.put("partition_format",val1);


        runner.enqueue("abcd",attributes);
        runner.run();

        MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(MDDIFPartitionBuilder.REL_SUCCESS).get(0);

        Map<String, String> writeAttributes = ((MockFlowFile) successFlowFile).getAttributes();
        for (Map.Entry<String,String> entry : writeAttributes.entrySet())
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());


    }


    @Test
    public void testEmptyPartitionBuilder() throws IOException {
        HashMap attributes = new HashMap();
        //attributes.put("field_separator","|");
        attributes.put("conf_id","1");
        attributes.put("file_uuid","1234");
        attributes.put("filename","test.xls");
        attributes.put("sequence_id","12345");
        attributes.put("partition_format","{\"partitionArr\":[]}");
        //attributes.put("partition_format","{\"partitionArr\":[{\"columnName\":\"year\", \"source\":\"date\", \"format\":\"yyyy\"},{\"columnName\":\"month\", \"source\":\"date\", \"format\":\"MM\"}, {\"columnName\":\"sequence_id\", \"source\":\"attribute\", \"attributeName\":\"sequence_id\"},{\"columnName\":\"load_date\", \"source\":\"dateWithMod\", \"format\":\"yyyyMMddHH\", \"appendMod\":\"mm15\"} ]}");

        String val = "{\"partitionArr\":[{\"columnName\":\"load_date\", \"source\":\"date\", \"format\":\"yyyyMMdd\"}]}";
        runner.enqueue("abcd",attributes);
        runner.run();

        MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(MDDIFPartitionBuilder.REL_SUCCESS).get(0);

        Map<String, String> writeAttributes = ((MockFlowFile) successFlowFile).getAttributes();
        for (Map.Entry<String,String> entry : writeAttributes.entrySet())
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());


    }

    @Test
    public void testBlankPartitionBuilder() throws IOException {
        HashMap<String, String> attributes = new HashMap();
        //attributes.put("field_separator","|");
        attributes.put("conf_id","1");
        attributes.put("file_uuid","1234");
        attributes.put("filename","test.xls");
        attributes.put("sequence_id","12345");
        attributes.put("partition_format","{\"partitionArr\":[{\"columnName\":\"year\", \"source\":\"date\", \"format\":\"yyyy\"},{\"columnName\":\"month\", \"source\":\"date\", \"format\":\"MM\"},{\"columnName\":\"day\", \"source\":\"date\", \"format\":\"dd\"}]}");
        //attributes.put("partition_format","{\"partitionArr\":[{\"columnName\":\"year\", \"source\":\"date\", \"format\":\"yyyy\"},{\"columnName\":\"month\", \"source\":\"date\", \"format\":\"MM\"}, {\"columnName\":\"sequence_id\", \"source\":\"attribute\", \"attributeName\":\"sequence_id\"},{\"columnName\":\"load_date\", \"source\":\"dateWithMod\", \"format\":\"yyyyMMddHH\", \"appendMod\":\"mm15\"} ]}");

        String val = "{\"partitionArr\":[{\"columnName\":\"load_date\", \"source\":\"date\", \"format\":\"yyyyMMdd\"}]}";
        runner.enqueue("abcd",attributes);
        runner.run();

        MockFlowFile successFlowFile = runner.getFlowFilesForRelationship(MDDIFPartitionBuilder.REL_SUCCESS).get(0);

        Map<String, String> writeAttributes = ((MockFlowFile) successFlowFile).getAttributes();
        for (Map.Entry<String,String> entry : writeAttributes.entrySet())
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
    }
}
