package com.o2.edh.processors;

import com.o2.edh.processors.mddif.scheduler.MDDIFScheduler;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestMDDIFScheduler {
    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(MDDIFScheduler.class);
        runner.setProperty(MDDIFScheduler.CRON_SCHEDULE,"${run_schedule}");
    }

    @Test
    public void testRunSchedule() {
        Map<String,String> attributes = new HashMap();
        attributes.put("run_schedule","0 59 23 24 12 ? 2020/5");
        attributes.put("conf_id","1");
        attributes.put("file_name","abc.csv");
        attributes.put("file_uuid","1eer34647yu");
        final String content = "hello world";
        runner.enqueue(content,attributes);
        runner.run();
        runner.assertTransferCount(MDDIFScheduler.REL_SUCCESS,1);
        final MockFlowFile mockFlowfile = runner.getFlowFilesForRelationship(MDDIFScheduler.REL_SUCCESS).get(0);
       // mockFlowfile.assertAttributeEquals("next_run","2020-03-20 09:30:00");

        Map<String, String> writeAttributes = mockFlowfile.getAttributes();
        for (Map.Entry<String,String> entry : writeAttributes.entrySet())
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());

    }

    @Test
    public void testFailedRunSchedule() {
        Map<String,String> attributes = new HashMap();
        attributes.put("run_schedule","0 59 23 24 12 ? 2020/5a");
        attributes.put("conf_id","1");
        attributes.put("file_name","abc.csv");
        attributes.put("file_uuid","1eer34647yu");
        final String content = "hello world";
        runner.enqueue(content,attributes);
        runner.run();
        runner.assertTransferCount(MDDIFScheduler.REL_FAILURE,1);
        runner.assertTransferCount(MDDIFScheduler.LOG_RELATIONSHIP,0);
        final MockFlowFile mockFlowfile = runner.getFlowFilesForRelationship(MDDIFScheduler.REL_FAILURE).get(0);
        //final MockFlowFile logFlowfile = runner.getFlowFilesForRelationship(MDDIFScheduler.LOG_RELATIONSHIP).get(0);
        Map<String, String> writeAttributes = mockFlowfile.getAttributes();
        for (Map.Entry<String,String> entry : writeAttributes.entrySet())
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());


        /*Map<String, String> writeLogAttributes = logFlowfile.getAttributes();
        for (Map.Entry<String,String> entry : writeLogAttributes.entrySet())
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());*/
    }
}
