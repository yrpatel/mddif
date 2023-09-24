package com.o2.edh.processors.mddif.fileconversion;

import com.o2.edh.processors.mddif.util.Log;
import com.o2.edh.processors.mddif.util.Logger;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.csv.CSVValidators;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Iterator;

import org.apache.nifi.processor.util.StandardValidators;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

@Tags({"excel", "csv", "poi"})
@CapabilityDescription("Consumes a Microsoft Excel document and converts each worksheet to csv. Each sheet from the incoming Excel " +
        "document will generate a new Flowfile that will be output from this processor. Each output Flowfile's contents will be formatted as a csv file " +
        "where the each row from the excel sheet is output as a newline in the csv file. This processor is currently only capable of processing .xls (HSSF '97(-2007) file format) documents " +
        " This processor also expects well formatted " + "CSV content and will not escape cell's containing invalid content such as newlines or additional commas.")

public class MDDIFExcelToCSV extends AbstractProcessor {

    public static final PropertyDescriptor FIELD_SEPERATOR = new PropertyDescriptor
            .Builder().name("field_separator")
            .displayName("Field Separator")
            .description("Delimiter type. ")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATE_FORMAT = new PropertyDescriptor
            .Builder().name("date_format")
            .displayName("Date Format")
            .description("Date Format for output cell")
            .required(false)
            .defaultValue("yyyyMMddHHmmss")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Excel data converted to csv")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to parse the Excel document")
            .build();
    public static final Relationship LOG_RELATIONSHIP = new Relationship.Builder()
            .name("log")
            .description("Log relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FIELD_SEPERATOR);
        properties.add(DATE_FORMAT);
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
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {


        final FlowFile[] inFlowFile = {session.get()};
        if (inFlowFile[0] == null) {
            return;
        }

        //get flowfile attributes
        final String fieldSeperator = context.getProperty(FIELD_SEPERATOR).evaluateAttributeExpressions(inFlowFile[0]).getValue();
        final String dateFormat = context.getProperty(DATE_FORMAT).evaluateAttributeExpressions(inFlowFile[0]).getValue();
        final String filename = inFlowFile[0].getAttribute("file_name");
        final  String confId = inFlowFile[0].getAttribute("conf_id");
        final  String fileUUID = inFlowFile[0].getAttribute("file_uuid");

        // [LOGGING]: Init Log
        Logger logger = new Logger(session,getLogger(),LOG_RELATIONSHIP);
        logger.generateLog(new Log(LogLevel.DEBUG,confId,fileUUID,filename,"127001","Logger initiated"));

        FlowFile outFlowFile = session.create(inFlowFile[0]);

        try {
            final boolean[] error = { false };

            outFlowFile = session.write(outFlowFile, outputStream -> {
                session.read(inFlowFile[0], inputStream -> {
                    try {
                        /**
                         * Convert the Excel file data into CSV file
                         * @param inputStream
                         */
                        String CVS_SEPERATOR_CHAR = fieldSeperator;
                        String NEW_LINE_CHARACTER = "\r\n";


                        //Long recordCount = 0L;
                        HSSFWorkbook myWorkBook = new HSSFWorkbook(new POIFSFileSystem(inputStream));

                        int sheetsToProcess = myWorkBook.getNumberOfSheets();
                        for (int sheet = 0; sheet < sheetsToProcess; sheet++) {
                            HSSFSheet mySheet = myWorkBook.getSheetAt(sheet);
                            Iterator rowIter = mySheet.rowIterator();
                            StringBuffer csvData =  new StringBuffer();
                            while (rowIter.hasNext()) {
                                HSSFRow myRow = (HSSFRow) rowIter.next();
                                for (int i = 0; i < myRow.getLastCellNum() - 1; i++) {
                                    csvData.append((getCellData(myRow.getCell(i), dateFormat).contains(CVS_SEPERATOR_CHAR) ?
                                            "\""+ getCellData(myRow.getCell(i), dateFormat)+"\"": getCellData(myRow.getCell(i), dateFormat)) + CVS_SEPERATOR_CHAR);
                                }
                                csvData.append(getCellData(myRow.getCell(myRow.getLastCellNum() - 1), dateFormat) + NEW_LINE_CHARACTER) ;
                                outputStream.write(csvData.toString().getBytes(StandardCharsets.UTF_8));
                                csvData.setLength(0);
                            }
                        }

                        logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUUID, filename, "227001", "File converted from Excel to CSV successfully"));

                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename, "327002", "Failed to convert File from Excel to CSV"));
                        error[0] = true;
                    }finally {
                        inputStream.close();
                        outputStream.close();
                    }
                });
            });

            if(error[0]){
                session.remove(outFlowFile);
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename, "327003", "Transferring to failure"),inFlowFile[0],REL_FAILURE);
            } else {
                //outFlowFile = session.putAttribute(outFlowFile, "filename", filename.split("\\.")[0] + ".csv");
                logger.generateLog(new Log(LogLevel.INFO, confId, fileUUID, filename, "227002", "Session transferred successfully"));
                session.remove(inFlowFile[0]);
                session.transfer(outFlowFile,REL_SUCCESS);
            }

        } catch(Exception e) {
            session.remove(outFlowFile);
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUUID, filename,"327002",
                    "Failed to convert File from Excel to CSV " + e.getClass().getName() + " reason : " + e.getMessage()),
                    inFlowFile[0], REL_FAILURE);

        }
    }

    /**
     * Get cell value based on the excel column data type
     * @param myCell
     * @return
     */
    public String getCellData( HSSFCell myCell, String dateFormat) {
        String cellData="";
        if ( myCell== null){
            cellData += "";;
        }else{
            switch(myCell.getCellType() ){
                case  HSSFCell.CELL_TYPE_STRING  :
                case  HSSFCell.CELL_TYPE_BOOLEAN  :
                    cellData +=  myCell.getRichStringCellValue ();
                    break;
                case HSSFCell.CELL_TYPE_NUMERIC :
                    cellData += getNumericValue(myCell, dateFormat);
                    break;
                case  HSSFCell.CELL_TYPE_FORMULA :
                    cellData +=  getFormulaValue(myCell, dateFormat);
                default:
                    cellData += "";
            }
        }
        return cellData;
    }

    /**
     * Get the formula value from a cell
     * @param myCell
     * @return
     * @throws Exception
     */
    public String getFormulaValue(HSSFCell myCell, String dateFormat){
        String cellData="";
        if ( myCell.getCachedFormulaResultType() == HSSFCell.CELL_TYPE_STRING  || myCell.getCellType () ==HSSFCell.CELL_TYPE_BOOLEAN) {
            cellData +=  myCell.getRichStringCellValue ();
        }else  if ( myCell.getCachedFormulaResultType() == HSSFCell.CELL_TYPE_NUMERIC ) {
            cellData += getNumericValue(myCell, dateFormat);
        }
        return cellData;
    }

    /**
     * Get the date or number value from a cell
     * @param myCell
     * @return
     * @throws Exception
     */
    public String getNumericValue(HSSFCell myCell, String dateFormat) {
        String cellData="";
        if ( HSSFDateUtil.isCellDateFormatted(myCell) ){
            cellData += new SimpleDateFormat(dateFormat).format(myCell.getDateCellValue()) ;
        }else{
            cellData += new BigDecimal(myCell.getNumericCellValue()).toString() ;
        }
        return cellData;
    }
}
