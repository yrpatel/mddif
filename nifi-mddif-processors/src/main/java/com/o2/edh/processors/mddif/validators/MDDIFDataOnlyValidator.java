package com.o2.edh.processors.mddif.validators;

import com.o2.edh.mddif.serialization.RecordReaderFactory;
import com.o2.edh.mddif.serialization.RecordSetWriterFactory;
import com.o2.edh.processors.mddif.util.Log;
import com.o2.edh.processors.mddif.util.Logger;
import com.o2.edh.processors.mddif.validators.helpers.MetadataFields;
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
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.validation.SchemaValidationContext;
import org.apache.nifi.schema.validation.StandardSchemaValidator;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.validation.RecordSchemaValidator;
import org.apache.nifi.serialization.record.validation.SchemaValidationResult;
import org.apache.nifi.serialization.record.validation.ValidationError;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class MDDIFDataOnlyValidator extends AbstractProcessor {

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records. "
                    + "Regardless of the Controller Service schema access configuration, "
                    + "the schema that is used to validate record is used to write the valid results.")
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
                    + "correct type (if possible, according to the type coercion supported by the Record Writer).  To be set as true for MDDIF")
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
        final String recordCountLocation = flowFile.getAttribute("record_count_location");
        final String recordCountFilter = flowFile.getAttribute("record_count_filter");
        final String confId = flowFile.getAttribute("conf_id");
        final String fileUuid = flowFile.getAttribute("file_uuid");
        final String fileId = flowFile.getAttribute("file_id");
        final String headerSchema = flowFile.getAttribute("header_schema");
        final String metadataHeaderRecord = flowFile.getAttribute("metadata_header_record");
        //get fieldSeparator from flowfile and send to record to string conversions
        final String fieldSeparator = flowFile.getAttribute("field_separator");
        final String fileUuidOrId = flowFile.getAttribute("file_uuid_or_id");

        Logger logger = new Logger(session, getLogger(), LOG_RELATIONSHIP);  // [LOGGING]: Init Log
        logger.generateLog(new Log(LogLevel.DEBUG,confId,fileUuid,fileName,"125302","Logger initiated"));

        RecordSetWriter validWriter = null;
        FlowFile validFlowFile = null;
        long recordCount = 0;

        InputStream in = null;
        RecordReader reader = null;
        try {
            in = session.read(flowFile);
            reader = readerFactory.createRecordReader(flowFile, in, getLogger());

            final RecordSchema readerValidationSchema = reader.getSchema(); //reader schema
            final SchemaValidationContext validationContext = new SchemaValidationContext(readerValidationSchema, allowExtraFields, strictTypeChecking);
            final RecordSchemaValidator validator = new StandardSchemaValidator(validationContext);


            final Schema.Parser parser = new Schema.Parser();
            final Schema writerAvroSchema = parser.parse(writerSchemaText); //writer schema
            final RecordSchema writerValidationSchema = AvroTypeUtil.createSchema(writerAvroSchema);

            final Schema.Parser readParser = new Schema.Parser();
            final Schema readerAvroSchema = readParser.parse(flowFile.getAttribute("table_schema"));


            boolean error = false;


            final Set<String> extraFields = new HashSet<>();
            final Set<String> missingFields = new HashSet<>();
            final Set<String> invalidFields = new HashSet<>();
            final Set<String> otherProblems = new HashSet<>();

            //flowfile attribute validations
            boolean isMetadataHeaderRecordValid = true;
            boolean isValidHeader = validateHeaderFooter(headerRecordCount,"Header Count", confId, fileUuid, fileName, logger);
            boolean isValidFooter = validateHeaderFooter(footerRecordCount,"Footer Count", confId, fileUuid, fileName, logger);
            boolean isRecordCountLocationValid = validateRecordCountLocation(recordCountLocation, confId, fileUuid, fileName, logger);
            boolean isRecordCountFilterValid = validateRecordCountFilter(recordCountFilter, recordCountLocation, isRecordCountLocationValid, confId, fileUuid, fileName, logger);
            boolean isAddFileAndRecordIdValid = validateAddFileAndRecordId(addFileAndRecordId, confId, fileUuid, fileName, logger);
            boolean isFileUuidOrIdValid = validateFileUuidOrId(fileUuidOrId, confId, fileUuid, fileName, logger);


            if(!isValidHeader || !isValidFooter || !isRecordCountLocationValid || !isAddFileAndRecordIdValid
                    || !isRecordCountFilterValid || !isFileUuidOrIdValid){
                closeQuietly(reader, in);
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325305",
                        "Attribute validation failed, sending file to failure relationship"), flowFile, REL_FAILURE);
                return;
            }

            int headerCount = Integer.parseInt(headerRecordCount);
            int footerCount = Integer.parseInt(footerRecordCount);

            //header_metadata, header_metadata_and_footer conditional validation
            int metadataHeaderRecordNumber = 0;
            if(metadataHeaderRecord != null && !metadataHeaderRecord.trim().equalsIgnoreCase("")) {
                if (metadataHeaderRecord.equalsIgnoreCase("header_metadata") || metadataHeaderRecord.equalsIgnoreCase("header_metadata_and_footer"))
                    isMetadataHeaderRecordValid = validateMetadataHeaderRecord(metadataHeaderRecord, isValidHeader, headerCount, confId, fileUuid, fileName, logger);

                if (!isMetadataHeaderRecordValid) {
                    closeQuietly(reader, in);
                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName, "325323",
                            "metadata_header_record validation failed, sending file to failure relationship"), flowFile, REL_FAILURE);
                    return;
                }
                metadataHeaderRecordNumber = Integer.parseInt(metadataHeaderRecord);
            }

            boolean addFileIdRecordIdFlag = (addFileAndRecordId.equals("1"));
            boolean headerRecordCountPresentFlag = false;
            String fileUuidFileId = fileUuidOrId.equalsIgnoreCase("file_uuid")?fileUuid:fileId;

            MetadataFields ManifestObj = null;

            try {
                Record record;
                ArrayList<Record> headerBuffer = new ArrayList<>();
                ArrayList<Record> footerBuffer = new ArrayList<>();
                Record currentRecord = null;
                error = false;
                RecordSetWriter writer;
                validFlowFile = session.create(flowFile);   //move out of while loop for performance enhancement
                validWriter = writer = createIfNecessary(validWriter, (RecordSetWriterFactory) validRecordWriterFactory, session, validFlowFile, writerValidationSchema); //move out of while loop for performance enhancement

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
                    if(recordCount <= headerCount){         //header skip 2
                        headerBuffer.add(record);
                        if(recordCount == metadataHeaderRecordNumber){ //2
                            //header schema
                            final Schema headerAvroSchema = parser.parse(headerSchema); //header metadata schema
                            final RecordSchema headerValidationSchema = AvroTypeUtil.createSchema(headerAvroSchema);
                            //final SchemaValidationContext headerValidationContext = new SchemaValidationContext(headerValidationSchema, allowExtraFields, strictTypeChecking);
                            //final RecordSchemaValidator headerValidator = new StandardSchemaValidator(headerValidationContext);

                            Map<String, Object> headerMap = new LinkedHashMap<String, Object>();

                            Iterator<String> headerItr = record.getRawFieldNames().iterator();
                            //record.getSchema().getFields()
                            Iterator<String> headerSchemaItr = headerValidationSchema.getFieldNames().iterator();
                            while(headerSchemaItr.hasNext()) {
                                if(!headerItr.hasNext()){
                                    //failure break (need clarification)
                                    closeQuietly(reader, in);
                                    closeQuietly(validWriter);
                                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325324",
                                            "header record schema has less elements than header metadata schema"), flowFile, REL_FAILURE);
                                    session.remove(validFlowFile);
                                    break;
                                }
                                String fieldName = headerItr.next();
                                Object rawValue = null;
                                String headerfield = headerSchemaItr.next();
                                if(headerfield.equalsIgnoreCase("record_count"))
                                    headerRecordCountPresentFlag = true;
                                try {
                                    if (headerfield.equals("sequence_number") || headerfield.equals("file_size") || headerfield.equals("record_count") || headerfield.equals("num_of_fields"))
                                        rawValue = Long.parseLong(String.valueOf(record.getValue(fieldName)));
                                    else if(headerfield.equals("data_end_date") || headerfield.equals("data_start_date") || headerfield.equals("date_of_file_generation")) {
                                        rawValue = String.valueOf(record.getValue(fieldName)==null?"":record.getValue(fieldName));
                                    } else
                                        rawValue = String.valueOf(record.getValue(fieldName)==null?"":record.getValue(fieldName));
                                }catch (final NumberFormatException e) {
                                    closeQuietly(reader, in);
                                    closeQuietly(validWriter);
                                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325325",
                                            "NumberFormatException in Metadata Header record " + e.getClass().getName() +" reason : "+ e.getMessage()),
                                            flowFile, REL_FAILURE);
                                    session.remove(validFlowFile);
                                    return;
                                }

                                headerMap.put(headerfield, rawValue);
                            }

                            if(headerItr.hasNext()){
                                //failure break (need clarification)
                                closeQuietly(reader, in);
                                closeQuietly(validWriter);
                                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325326",
                                        "header record schema has more elements than header metadata schema"), flowFile, REL_FAILURE);
                                session.remove(validFlowFile);
                                return;
                            }

                            //header record
                            Record headerRecord =  new MapRecord(headerValidationSchema, headerMap);
                                ManifestObj = MetadataFields.getManifestFields(headerRecord, headerValidationSchema,
                                        confId, fileUuid, fileName, logger);

                        }
                    }else {                        //actual data and its validation
                        footerBuffer.add(record);
                        if (footerBuffer.size() == footerCount + 1) {
                            currentRecord = footerBuffer.remove(0);
                            //recordToString(currentRecord);

                            //date timestamp to epoch conversion
                            for (Schema.Field reformatField : reformatFields) {
                                    String rawValue = String.valueOf(currentRecord.getValue(reformatField.name()));
                                if (!rawValue.equalsIgnoreCase("null")) {
                                    Date date = new SimpleDateFormat(reformatField.doc()).parse(rawValue);
                                    currentRecord.setValue(reformatField.name(), date.getTime());
                                }
                            }

                            final SchemaValidationResult result = validator.validate(currentRecord);
                            if (result.isValid()) {
                                //new updated record
                                Record writeRecord = null;
                                if(addFileIdRecordIdFlag){
                                    LinkedHashMap<String, Object> writeMap = new LinkedHashMap<>();
                                    writeMap.put("file_id", fileUuidFileId);    //add file_uuid or file_id
                                    writeMap.put("record_id", recordCount - headerCount - footerCount);
                                    writeMap.putAll(currentRecord.toMap());
                                    writeRecord = new MapRecord(writerValidationSchema, writeMap);
                                }else
                                    writeRecord = currentRecord;

                                if (writer instanceof RawRecordWriter) {
                                    ((RawRecordWriter) writer).writeRawRecord(writeRecord);
                                } else {
                                    writer.write(writeRecord);
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

                //actual record count and manifest count matching
                boolean recordCountCheckByMatcher = !recordCountLocation.equalsIgnoreCase("none") && !recordCountLocation.equalsIgnoreCase("header_metadata");
                Matcher matcher = null;
                long receivedRecordCount = 0L;
                if(recordCountCheckByMatcher) {
                    Pattern pattern = Pattern.compile(recordCountFilter);
                    switch (recordCountLocation.toLowerCase()) {
                        case "header":
                            matcher = pattern.matcher(arrayToString(headerBuffer, fieldSeparator)); //field1|field2|field3
                            break;
                        case "footer":
                        case "header_metadata_and_footer":
                            matcher = pattern.matcher(arrayToString(footerBuffer, fieldSeparator));
                            break;
                        case "filename":
                            matcher = pattern.matcher(fileName);
                            break;
                    }

                    if(matcher.find()) {
                        String receivedRecordCountString = matcher.group();
                        getLogger().info("recordCountLocation ::" + matcher + "receivedRecordCountString :: " + receivedRecordCountString);
                        try {
                            receivedRecordCount = Long.parseLong(receivedRecordCountString);
                        } catch (NumberFormatException e) {
                            closeQuietly(reader, in);
                            closeQuietly(validWriter);
                            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325307",
                                    "Invalid receivedRecordCount; Transferring file to failure")
                                    ,flowFile,REL_FAILURE);
                            session.remove(validFlowFile);
                            return;
                        }

                    }else{
                        //pattern does not matches failure
                        //return
                        closeQuietly(reader, in);
                        closeQuietly(validWriter);
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325327",
                                "record count filter failed to extract record count"), flowFile, REL_FAILURE);
                        session.remove(validFlowFile);
                        return;
                    }
                }


                if(recordCountLocation.equalsIgnoreCase("header_metadata") && ManifestObj!=null ){
                    if(ManifestObj.getRecord_count()!=null) {
                        receivedRecordCount = ManifestObj.getRecord_count();
                    }else {
                        //failure; return
                        closeQuietly(reader, in);
                        closeQuietly(validWriter);
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325328",
                                "failed to retreive record count from header metadata"), flowFile, REL_FAILURE);
                        session.remove(validFlowFile);
                        return;
                    }
                }else if(recordCountLocation.equalsIgnoreCase("header_metadata_and_footer") && ManifestObj!=null && headerRecordCountPresentFlag ){
                    if(ManifestObj.getRecord_count() == null ) {
                        //failure; return
                        closeQuietly(reader, in);
                        closeQuietly(validWriter);
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325328",
                                "failed to retreive record count from header metadata"), flowFile, REL_FAILURE);
                        session.remove(validFlowFile);
                        return;
                    }else if(ManifestObj.getRecord_count() != receivedRecordCount){
                        //failure with record count at header metadata and footer does not match
                        //return
                        closeQuietly(reader, in);
                        closeQuietly(validWriter);
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325329",
                                "record count at header metadata :: "+ ManifestObj.getRecord_count() +" and footer :: "+receivedRecordCount+" does not match"), flowFile, REL_FAILURE);
                        session.remove(validFlowFile);
                        return;
                    }

                }else if((recordCountLocation.equalsIgnoreCase("header_metadata") || recordCountLocation.equalsIgnoreCase("header_metadata_and_footer")) && ManifestObj==null){
                    //header metadata not found; send failure; return
                    closeQuietly(reader, in);
                    closeQuietly(validWriter);
                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325330",
                            "header metadata not found"), flowFile, REL_FAILURE);
                    session.remove(validFlowFile);
                    return;
                }

                //actual and received record count match
                long actualRecordCount = recordCount - headerCount - footerCount;
                if(!recordCountLocation.equalsIgnoreCase("none")) {
                    if (receivedRecordCount == actualRecordCount) {
                        logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUuid, fileName,
                                "125303",       //identify LogTypeId from logType Table for validation failure
                                "RECORD COUNTS MATCHED. Received Record Count : " + receivedRecordCount + " Actual Record Count : " + actualRecordCount)
                        );
                    } else {
                        closeQuietly(reader, in);
                        closeQuietly(validWriter);
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName, "325306",
                                        "RECORD COUNTS DO NOT MATCHED. Received Record Count : " + receivedRecordCount + " Actual Record Count : " + actualRecordCount),
                                flowFile, REL_FAILURE);
                        session.remove(validFlowFile);
                        return;
                    }
                }


                if (validWriter != null && !error) {

                    closeQuietly(reader, in);
                    closeQuietly(validWriter);
                    if(ManifestObj==null)
                        ManifestObj = new MetadataFields();
                    validFlowFile = session.putAttribute(validFlowFile, "metadata_file_name", ((ManifestObj.getFile_name() == null) ? "" : ManifestObj.getFile_name()));
                    validFlowFile = session.putAttribute(validFlowFile, "metadata_sequence_number", ((ManifestObj.getSequenceNumber() == null) ? "" : ManifestObj.getSequenceNumber().toString()));
                    validFlowFile = session.putAttribute(validFlowFile, "metadata_file_size", ((ManifestObj.getFile_size() == null) ? "" : ManifestObj.getFile_size().toString()));
                    validFlowFile = session.putAttribute(validFlowFile, "metadata_record_count", ((ManifestObj.getRecord_count() == null) ? "" : ManifestObj.getRecord_count().toString()));
                    validFlowFile = session.putAttribute(validFlowFile, "metadata_number_of_fields", ((ManifestObj.getNum_of_fields() == null) ? "" : ManifestObj.getNum_of_fields().toString()));
                    validFlowFile = session.putAttribute(validFlowFile, "metadata_start_date", ((ManifestObj.getData_start_date() == null) ? "" : ManifestObj.getData_start_date()));
                    validFlowFile = session.putAttribute(validFlowFile, "metadata_end_date", ((ManifestObj.getData_end_date() == null) ? "" : ManifestObj.getData_end_date()));
                    validFlowFile = session.putAttribute(validFlowFile, "metadata_Date_of_file_generation", ((ManifestObj.getDate_of_file_generation() == null) ? "" : ManifestObj.getDate_of_file_generation()));
                    validFlowFile = session.putAttribute(validFlowFile, "actual_file_size", ""+flowFile.getSize());
                    validFlowFile = session.putAttribute(validFlowFile, "actual_record_count", ""+actualRecordCount);

                    logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName,"225304","File is validated successfully; transferring File to success"));
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
                    //custom logger for validation failure
                    closeQuietly(reader, in);
                    closeQuietly(validWriter);
                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325308",
                            (validationErrorString.length()>65534) ? validationErrorString.substring(0,65535) : validationErrorString), flowFile, REL_FAILURE);
                    session.remove(validFlowFile);
                }
            } finally{
                closeQuietly(reader, in);
                closeQuietly(validWriter);
            }

        }catch (final NullPointerException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325333","NullPointerException " + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        }  catch (IllegalArgumentException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325334","IllegalArgumentException " + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        }catch (ParseException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325335","Date format incorrect in avro doc" + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        }
        catch (final MalformedRecordException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325309","MalformedRecordException at record " + (recordCount+1) +" "+ getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        } catch (SchemaNotFoundException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325310","SchemaNotFoundException " + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        } catch (SchemaParseException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325311","SchemaParseException " + getExceptionString(e)), flowFile, REL_FAILURE);
            if (validFlowFile != null) session.remove(validFlowFile);
        } catch (IOException e) {
            closeQuietly(reader, in);
            closeQuietly(validWriter);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325312","IOException " + getExceptionString(e)), flowFile, REL_FAILURE);
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

        //final RecordSetWriter created = factory.createWriter(getLogger(), outputSchema, out);
        //created.beginRecordSet();
        //return created;
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
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325313",       //identify LogTypeId from logType Table for invalid header footer count
                    sb.toString())
            );
            getLogger().debug(sb.toString());
        }
    }

    private String arrayToString(ArrayList<Record> recordList, String fieldSeparator){
        Iterator<Record> itr = recordList.iterator();
        String lines = "";
        if(recordList.size()>0) {
            while (itr.hasNext()) {
                lines = lines + recordToString(itr.next(), fieldSeparator) + "\n";
            }
            return lines.substring(0, lines.length() - 1);
        }
        return null;
    }

    private String recordToString(Record record, String fieldSeparator){
        String fieldNames;
        String line = "";
        Set<String> fieldNameSet = record.getRawFieldNames();
        for (String s : fieldNameSet) {
            fieldNames = s;
            line = line + record.getAsString(fieldNames) + fieldSeparator;
        }
        return line;
    }

    private boolean validateRecordCountFilter(String recordCountFilter, String recordCountLocation, boolean isRecordCountLocationValid, String confId, String fileUuid, String fileName, Logger logger) {

        if(isRecordCountLocationValid && !recordCountLocation.equalsIgnoreCase("none") && !recordCountLocation.equalsIgnoreCase("header_metadata")
                && (recordCountFilter == null || recordCountFilter.equals(""))){
            String message = "Invalid record_count_filter attribute value";
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325331", message));
            return false;
        }
        return true;
    }

    private boolean validateRecordCountLocation(String recordCountLocation, String confId, String fileUuid, String fileName, Logger logger) {
        if(!recordCountLocation.equalsIgnoreCase("header") &&
                !recordCountLocation.equalsIgnoreCase("footer") &&
                !recordCountLocation.equalsIgnoreCase("filename") &&
                !recordCountLocation.equalsIgnoreCase("header_metadata") &&
                !recordCountLocation.equalsIgnoreCase("header_metadata_and_footer") &&
                !recordCountLocation.equalsIgnoreCase("none") ){
            String message = "Invalid record_count_location attribute value, allowed value are header, footer, filename, header_metadata, header_metadata_and_footer or none";
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325314", message));
            return false;
        }
        return true;
    }

    private boolean validateAddFileAndRecordId(String addFileAndRecordId, String confId, String fileUuid, String fileName, Logger logger) {
        if(!addFileAndRecordId.equalsIgnoreCase("1") &&
                !addFileAndRecordId.equalsIgnoreCase("0")){
            String message = "Invalid add_file_and_record_id attribute value, allowed value are 1 or 0";
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325315", message));
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
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325316", message));
            //identify LogTypeId from logType Table for invalid header footer count
            return false;
        }

    }

    private boolean validateMetadataHeaderRecord(String metadataHeaderRecord, boolean isValidHeader, int headerCount, String confId, String fileUuid, String fileName, Logger logger) {

        try{
            if (isValidHeader) {
                int metadataHeaderRecords = Integer.parseInt(metadataHeaderRecord); //"" null
                //valid since it is a number and less than header record count
                //invalid since it is a number and less than header record count
                return metadataHeaderRecords < headerCount;
            }
        }catch (NumberFormatException e){
            String message ="Invalid metadata_header_record attribute value, value should be a Number ";
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"325332", message));
            //identify LogTypeId from logType Table for invalid header footer count
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
        return stackTrace;
    }
}