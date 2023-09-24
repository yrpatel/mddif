package com.o2.edh.processors.mddif.scheduler;


import com.o2.edh.processors.mddif.util.Log;
import com.o2.edh.processors.mddif.util.Logger;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.quartz.CronExpression;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 Created on 03-02-2020
 */
@Tags({"CRON","Scheduling"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MDDIFScheduler extends AbstractProcessor {

    public static final PropertyDescriptor CRON_SCHEDULE = new PropertyDescriptor.Builder()
            .name("CronSchedule")
            .description("CronSchedule")
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
        propertyDescriptors.add(CRON_SCHEDULE);
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inFlowFile = session.get();
        if ( inFlowFile == null ) {
            return;
        }

        //get flowfile attributes
        final String cron_Schedule = context.getProperty(CRON_SCHEDULE).evaluateAttributeExpressions(inFlowFile).getValue();
        final String filename = inFlowFile.getAttribute("file_name");
        final String confId = inFlowFile.getAttribute("conf_id");
        final String fileUUID = inFlowFile.getAttribute("file_uuid");

        Logger logger = new Logger(session,getLogger(),LOG_RELATIONSHIP);  // [LOGGING]: Init Log
        logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUUID, filename,"110029", "Logger initiated for MDDIFScheduler" ));

        String nextRun;
        CronExpression cron;
        try {
            cron = new CronExpression(cron_Schedule);
        } catch (ParseException e) {
            e.printStackTrace();
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"310032",
                    "Failed to evaluate Cron Expression " + e.getClass().getName() + " reason : "+e.getMessage()),
                    inFlowFile,REL_FAILURE );
            return;
        }

        Date currentTime = new Date();
        Date nextExecution = cron.getNextValidTimeAfter(currentTime);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        nextRun = formatter.format(nextExecution);

        inFlowFile= session.putAttribute(inFlowFile,"next_run",nextRun);
        session.transfer(inFlowFile, REL_SUCCESS);
        logger.generateLog(new Log(LogLevel.INFO, confId, fileUUID, filename,"210031",
                "Successfully evaluated the Cron Schedule " ));

    }
}