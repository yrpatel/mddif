package com.o2.edh.processors.mddif.validators;

import com.o2.edh.mddif.serialization.RecordReaderFactory;
import com.o2.edh.mddif.serialization.RecordSetWriterFactory;
import com.o2.edh.processors.mddif.util.Log;
import com.o2.edh.processors.mddif.util.Logger;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.validation.SchemaValidationContext;
import org.apache.nifi.schema.validation.StandardSchemaValidator;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.validation.RecordSchemaValidator;
import org.apache.nifi.serialization.record.validation.SchemaValidationResult;
import org.apache.nifi.serialization.record.validation.ValidationError;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record", "schema", "validate"})
@CapabilityDescription("Validates the Records of an incoming FlowFile against a given schema i.e. \"table_schema\"."
        +"Files that adhere to the schema are routed to the \"success\" relationship while "
        + "files having any single record that do not adhere to the schema or having invalid attributes are routed to the \"failure\" relationship. "
        + "Logs for each step of validation along with details are routed to the \"log\" relationship")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile routed to a relationship")
})
public class MDDIFDataValidator extends AbstractProcessor {

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            //.identifiesControllerService(RecordReaderFactory.class)
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records. "
                    + "Regardless of the Controller Service schema access configuration, "
                    + "the schema that is used to validate record is used to write the valid results.")
            //.identifiesControllerService(RecordSetWriterFactory.class)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();
    public static final PropertyDescriptor ALLOW_EXTRA_FIELDS = new PropertyDescriptor.Builder()
            .name("allow-extra-fields")
            .displayName("Allow Extra Fields")
            .description("If the incoming data has fields that are not present in the schema, this property determines whether or not the Record is valid. "
                    + "If true, the Record is still valid. If false, the Record will be invalid due to the extra fields. To be set as false for MDDIF")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    public static final PropertyDescriptor STRICT_TYPE_CHECKING = new PropertyDescriptor.Builder()
            .name("strict-type-checking")
            .displayName("Strict Type Checking")
            .description("If the incoming data has a Record where a field is not of the correct type, this property determine whether how to handle the Record. "
                    + "If true, the Record will still be considered invalid. If false, the Record will be considered valid and the field will be coerced into the "
                    + "correct type (if possible, according to the type coercion supported by the Record Writer). To be set as true for MDDIF")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Records that are valid according to the schema will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If the records cannot be read, validated, or written, for any reason, the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship LOG_RELATIONSHIP = new Relationship.Builder()
            .name("log")
            .description("Log relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(ALLOW_EXTRA_FIELDS);
        properties.add(STRICT_TYPE_CHECKING);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordSetWriterFactory validRecordWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        final boolean allowExtraFields = context.getProperty(ALLOW_EXTRA_FIELDS).asBoolean();
        final boolean strictTypeChecking = context.getProperty(STRICT_TYPE_CHECKING).asBoolean();

        //get flowfile attributes
        final String addFileAndRecordId = flowFile.getAttribute("add_file_and_record_id");
        final String writerSchemaText = flowFile.getAttribute("insert_table_schema");
        final String fileName = flowFile.getAttribute("file_name");
        final String headerRecordCount = flowFile.getAttribute("header_record_count");
        final String footerRecordCount = flowFile.getAttribute("footer_record_count");
        final String recordCountLocation = flowFile.getAttribute("record_count_location");  //straight away ignore it with debug
        final String recordCountFilter = flowFile.getAttribute("record_count_filter"); //not null check and other checks with debug log
        final String confId = flowFile.getAttribute("conf_id");
        final String fileUuid = flowFile.getAttribute("file_uuid");
        final String fileId = flowFile.getAttribute("file_id");
        final String metadataRecordCount = flowFile.getAttribute("metadata_record_count"); //null blank non_number
        final String fileUuidOrId = flowFile.getAttribute("file_uuid_or_id");
        final long actualFileSize = flowFile.getSize();

        Logger logger = new Logger(session, getLogger(), LOG_RELATIONSHIP);  // [LOGGING]: Init Log
        logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUuid, fileName,"125202","Logger initiated"));

        RecordSetWriter validWriter = null;
        FlowFile validFlowFile = null;

        long recordCount = 0L;
        InputStream in = null;
        RecordReader reader = null;
        try  {

            in = session.read(flowFile);
            reader = readerFactory.createRecordReader(flowFile, in, getLogger());

            final RecordSchema readerValidationSchema = reader.getSchema(); //exception
            final SchemaValidationContext validationContext = new SchemaValidationContext(readerValidationSchema, allowExtraFields, strictTypeChecking);
            final RecordSchemaValidator validator = new StandardSchemaValidator(validationContext);

            final Schema.Parser parser = new Schema.Parser();
            final Schema writerAvroSchema = parser.parse(writerSchemaText); //exception
            final RecordSchema writerValidationSchema = AvroTypeUtil.createSchema(writerAvroSchema);

            final Schema.Parser readParser = new Schema.Parser();
            final Schema readerAvroSchema = readParser.parse(flowFile.getAttribute("table_schema"));

            boolean error = false;

            final Set<String> extraFields = new HashSet<>();
            final Set<String> missingFields = new HashSet<>();
            final Set<String> invalidFields = new HashSet<>();
            final Set<String> otherProblems = new HashSet<>();

            //flowfile attribute validations
            boolean isValidHeader = validateHeaderFooter(headerRecordCount,"Header Count", confId, fileUuid, fileName, logger);
            boolean isValidFooter = validateHeaderFooter(footerRecordCount,"Footer Count", confId, fileUuid, fileName, logger);
            boolean isValidMetadataRecordCount = validateHeaderFooter(metadataRecordCount,"Metadata Record Count", confId, fileUuid, fileName, logger);
            boolean isRecordCountLocationValid = validateRecordCountLocation(recordCountLocation, confId, fileUuid, fileName, logger); //is it required
            boolean isRecordCountFilterValid = validateRecordCountFilter(recordCountFilter, recordCountLocation, isRecordCountLocationValid, confId, fileUuid, fileName, logger);
            boolean isAddFileAndRecordIdValid = validateAddFileAndRecordId(addFileAndRecordId, confId, fileUuid, fileName, logger);
            boolean isFileUuidOrIdValid = validateFileUuidOrId(fileUuidOrId, confId, fileUuid, fileName, logger);

            if(!isValidHeader || !isValidFooter || !isRecordCountLocationValid || !isAddFileAndRecordIdValid || !isValidMetadataRecordCount
                    || !isRecordCountFilterValid || !isFileUuidOrIdValid){
                closeQuietly(reader,in);
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325205",
                        "Attribute validation failed, sending file to failure relationship"), flowFile, REL_FAILURE);
                return;
            }

            int headerCount = Integer.parseInt(headerRecordCount);
            int footerCount = Integer.parseInt(footerRecordCount);
            boolean addFileIdRecordIdFlag = (addFileAndRecordId.equals("1"));

            String fileUuidFileId = fileUuidOrId.equalsIgnoreCase("file_uuid")?fileUuid:fileId;

            try {
                Record record;
                ArrayList<Record> headerBuffer = new ArrayList<>();
                ArrayList<Record> footerBuffer = new ArrayList<>();
                Record currentRecord = null;
                Record writeRecord = null;
                error = false;
                validFlowFile = session.create(flowFile);
                validWriter = createIfNecessary(validWriter, validRecordWriterFactory, session, validFlowFile, writerValidationSchema);

                String logicalType;
                List<Schema.Field> reformatFields = new ArrayList<>();

                //reader field details
                List<Schema.Field> fieldNames = readerAvroSchema.getFields();
                Iterator<Schema.Field> itr = fieldNames.iterator();
                Schema.Field field;
                while (itr.hasNext()) {
                    field = itr.next();
                    logicalType = field.getProp("logicalType");
                    if(logicalType != null) {
                        if (logicalType.equals("date") || logicalType.equals("time-millis") || logicalType.equals("time-micros") ||
                                logicalType.equals("timestamp-millis") || logicalType.equals("timestamp-micros")) {
                            reformatFields.add(field);
                        }
                    }
                }

                //while loop to read each record.
                while ((record = reader.nextRecord(false, false)) != null) {

                    recordCount++;
                    if(recordCount <= headerCount){         //header skip
                        headerBuffer.add(record);
                    }else {                        //actual data and its validation
                        footerBuffer.add(record);
                        if (footerBuffer.size() == footerCount + 1) {
                            currentRecord = footerBuffer.remove(0);
                            //recordToString(currentRecord);

                            //date timestamp to epoch conversion
                            for (Schema.Field reformatField : reformatFields) {
                                    String rawValue = String.valueOf(currentRecord.getValue(reformatField.name()));
                                //mandatory fields date conversion
                                if (!rawValue.equalsIgnoreCase("null")) {
                                    Date date = new SimpleDateFormat(reformatField.doc()).parse(rawValue);
                                    currentRecord.setValue(reformatField.name(), date.getTime());
                                }

                            }

                            final SchemaValidationResult result = validator.validate(currentRecord);
                            if (result.isValid()) {
                                //new updated record
                                if(addFileIdRecordIdFlag){
                                    LinkedHashMap<String, Object> writeMap = new LinkedHashMap<>();
                                    writeMap.put("file_id", fileUuidFileId);   //add file_uuid or file_id
                                    writeMap.put("record_id", recordCount - headerCount - footerCount);
                                    writeMap.putAll(currentRecord.toMap());
                                    writeRecord = new MapRecord(writerValidationSchema, writeMap);
                                }else
                                    writeRecord = currentRecord;

                                if (validWriter instanceof RawRecordWriter) {
                                    ((RawRecordWriter) validWriter).writeRawRecord(writeRecord);
                                } else {
                                    validWriter.write(writeRecord);
                                }

                            } else {
                                logValidationErrors(flowFile, recordCount, result, confId, fileUuid, fileName, logger);
                                error = true;
                                // Add all of the validation errors to our Set<ValidationError> but only keep up to MAX_VALIDATION_ERRORS because if
                                // we keep too many then we both use up a lot of heap and risk outputting so much information in the Provenance Event
                                // that it is too noisy to be useful.
                                for (final ValidationError validationError : result.getValidationErrors()) {
                                    final Optional<String> fieldName = validationError.getFieldName();

                                    switch (validationError.getType()) {
                                        case EXTRA_FIELD:
                                            if (fieldName.isPresent()) {
                                                extraFields.add(fieldName.get());
                                            } else {
                                                otherProblems.add(validationError.getExplanation());
                                            }
                                            break;
                                        case MISSING_FIELD:
                                            if (fieldName.isPresent()) {
                                                missingFields.add(fieldName.get());
                                            } else {
                                                otherProblems.add(validationError.getExplanation());
                                            }
                                            break;
                                        case INVALID_FIELD:
                                            if (fieldName.isPresent()) {
                                                invalidFields.add(fieldName.get());
                                            } else {
                                                otherProblems.add(validationError.getExplanation());
                                            }
                                            break;
                                        case OTHER:
                                            otherProblems.add(validationError.getExplanation());
                                            break;
                                    }
                                }
                            }
                        }
                    }

                }

                //actual record count and metadata record count matching
                    long receivedRecordCount = Long.parseLong(metadataRecordCount);
                    long actualRecordCount = recordCount - headerCount - footerCount;
                    if (receivedRecordCount==actualRecordCount) {
                        logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUuid, fileName,
                                "125203",       //identify LogTypeId from logType Table for validation failure
                                "RECORD COUNTS MATCHED. Received Record Count : " + receivedRecordCount + " Actual Record Count : " + actualRecordCount)
                        );
                    } else {
                        closeQuietly(reader,in);
                        closeQuietly(validWriter);
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325206",
                                        "RECORD COUNTS DID NOT MATCH. Received Record Count : " + receivedRecordCount + " Actual Record Count : " + actualRecordCount),
                                flowFile, REL_FAILURE);
                        session.remove(validFlowFile);
                        return;
                    }

                if (validWriter != null && !error) {
                    logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName,"225204","File is validated successfully; transferring File to success"));
                    closeQuietly(reader,in);
                    closeQuietly(validWriter);
                    //add actual file size and actual record count attributes
                    validFlowFile = session.putAttribute(validFlowFile,"actual_file_size", ""+flowFile.getSize());
                    validFlowFile = session.putAttribute(validFlowFile,"actual_record_count", ""+actualRecordCount);
                    validFlowFile = session.putAttribute(validFlowFile,"actual_file_size", ""+actualFileSize);
                    session.transfer(validFlowFile, REL_SUCCESS);   //validFlowFile removed header and footer hence sent to success.
                    session.remove(flowFile);
                    return;
                }

                if(error) {
                    // Build up a String that explains why the records were invalid, so that we can add this to the Provenance Event.
                    final StringBuilder errorBuilder = new StringBuilder();
                    errorBuilder.append("Records in this FlowFile were invalid for the following reasons: ");
                    if (!missingFields.isEmpty()) {
                        errorBuilder.append("The following ").append(missingFields.size()).append(" fields were missing: ").append(missingFields.toString());
                    }

                    if (!extraFields.isEmpty()) {
                        if (errorBuilder.length() > 0) {
                            errorBuilder.append("; ");
                        }

                        errorBuilder.append("The following ").append(extraFields.size())
                                .append(" fields were present in the Record but not in the schema: ").append(extraFields.toString());
                    }

                    if (!invalidFields.isEmpty()) {
                        if (errorBuilder.length() > 0) {
                            errorBuilder.append("; ");
                        }

                        errorBuilder.append("The following ").append(invalidFields.size())
                                .append(" fields had values whose type did not match the schema: ").append(invalidFields.toString());
                    }

                    if (!otherProblems.isEmpty()) {
                        if (errorBuilder.length() > 0) {
                            errorBuilder.append("; ");
                        }

                        errorBuilder.append("The following ").append(otherProblems.size())
                                .append(" additional problems were encountered: ").append(otherProblems.toString());
                    }

                    final String validationErrorString = errorBuilder.toString();
                    getLogger().error(validationErrorString);
                    //custom logger for validation failure
                    closeQuietly(reader,in);
                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325208",
                            (validationErrorString.length()>65534) ? validationErrorString.substring(0,65535) : validationErrorString), flowFile, REL_FAILURE);
                    closeQuietly(validWriter);
                    session.remove(validFlowFile);
                }
            } finally {
                closeQuietly(reader,in);
                closeQuietly(validWriter);
            }

        }
        catch (final NullPointerException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325218","NullPointerException " + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        }catch (IllegalArgumentException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325223","IllegalArgumentException " + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        }catch (ParseException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325224","Date format incorrect in avro doc" + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        }
        catch (final MalformedRecordException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325209","MalformedRecordException at record " + (recordCount+1) +" "+ getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        } catch (SchemaNotFoundException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325210","SchemaNotFoundException " + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        } catch (SchemaParseException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325211","SchemaParseException " + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        } catch (IOException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325212","IOException " + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        }

    }

    private void closeQuietly(final RecordSetWriter writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (final IOException e) {
                getLogger().error("Failed to close Record Writer", e);
            }
        }
    }

    private void closeQuietly(final RecordReader reader, final InputStream in) {
        if (reader != null) {
            try {
                reader.close();
            } catch (final IOException e) {
                getLogger().error("Failed to close Record Reader", e);
            }
        }

        if(in != null){
            try{
                in.close();
            }catch(final IOException e){
                getLogger().error("Failed to close Input Stream", e);
            }
        }

    }

    private RecordSetWriter createIfNecessary(final RecordSetWriter writer, final RecordSetWriterFactory factory, final ProcessSession session,
                                              final FlowFile flowFile, final RecordSchema outputSchema) throws SchemaNotFoundException, IOException {
        if (writer != null) {
            return writer;
        }

        final OutputStream out = session.write(flowFile);

        final RecordSetWriter created = factory.createWriter(getLogger(), outputSchema, out, flowFile);
        created.beginRecordSet();
        return created;
    }

    private void logValidationErrors(final FlowFile flowFile, final long recordCount, final SchemaValidationResult result,
                                     String confId, String fileUuid, String fileName, Logger logger) {
        if (getLogger().isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("For ").append(flowFile).append(" Record #").append(recordCount).append(" is invalid due to:\n");
            for (final ValidationError error : result.getValidationErrors()) {
                sb.append(error).append("\n");
            }
            // is debug appropriate here.
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325213",       //identify LogTypeId from logType Table for invalid header footer count
                    sb.toString())
            );
            getLogger().debug(sb.toString());
        }
    }

    private boolean validateRecordCountLocation(String recordCountLocation, String confId, String fileUuid, String fileName, Logger logger) {
        if(!recordCountLocation.equalsIgnoreCase("header") &&
                !recordCountLocation.equalsIgnoreCase("footer") &&
                !recordCountLocation.equalsIgnoreCase("filename") &&
                !recordCountLocation.equalsIgnoreCase("none") ){
            String message = "Invalid record_count_location attribute value, allowed value are header, footer, filename or none";
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325214", message));
            return false;
        }
        return true;
    }

    private boolean validateAddFileAndRecordId(String addFileAndRecordId, String confId, String fileUuid, String fileName, Logger logger) {
        if(!addFileAndRecordId.equalsIgnoreCase("1") &&
                !addFileAndRecordId.equalsIgnoreCase("0")){
            String message = "Invalid add_file_and_record_id attribute value, allowed value are 1 or 0";
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325215", message));
            return false;
        }
        return true;
    }

    private boolean validateHeaderFooter(String attribute, String attributeName, String confId, String fileUuid, String fileName, Logger logger){
        try {
            int val = Integer.parseInt(attribute);
            return true;
        }catch (NumberFormatException e){
            String message ="Invalid "+  attributeName + " attribute value, value should be a Number ";
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325216", message));
            //identify LogTypeId from logType Table for invalid header footer count
            return false;
        }

    }

    private boolean validateRecordCountFilter(String recordCountFilter, String recordCountLocation, boolean isRecordCountLocationValid, String confId, String fileUuid, String fileName, Logger logger) {

        if(isRecordCountLocationValid && !recordCountLocation.equalsIgnoreCase("none") && (recordCountFilter == null || recordCountFilter.equals(""))){
            String message = "Invalid record_count_filter attribute value";
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325131", message));
            return false;
        }
        return true;
    }

    //New validation added needs to find logs for it
    private boolean validateFileUuidOrId(String fileUuidOrId, String confId, String fileUuid, String fileName, Logger logger) {
        if(!fileUuidOrId.equalsIgnoreCase("file_uuid") &&
                !fileUuidOrId.equalsIgnoreCase("file_id") ){
            String message = "Invalid file_uuid_or_id attribute value, allowed value are file_uuid or file_id";
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325314", message));
            return false;
        }
        return true;
    }

    private String getExceptionString(Exception e){
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        String[] error = errors.toString().split("\n");
        String stackTrace = "";

        for(int i = 0 ; i < error.length; i++){
            if(error[i].contains("com.o2.edh.processors.mddif.validators")){
                stackTrace = stackTrace + error[i];
                if ((i - 1) >= 0) {
                    stackTrace = stackTrace + error[i-1];
                    break;
                }
                break;
            }
        }
        return stackTrace.toString();
    }

}