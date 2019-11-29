/*
 *
 * Copyright (c) 2019. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
 *
 */
package com.denodo.connect.hadoop.hdfs.util.schema;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.reader.HDFSParquetFileReader;
import com.denodo.connect.hadoop.hdfs.util.io.FileFilter;
import com.denodo.vdb.engine.customwrapper.condition.*;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HDFSParquetSchemaReader implements Iterator {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSParquetFileReader.class);

    private List<CustomWrapperFieldExpression> projectedFields;
    private MessageType parquetSchema;
    private boolean hasNullValueInConditions;
    private List<String> conditionFields;

    private Configuration configuration;

    private Path outputPath;
    private Path currentPath;
    private RemoteIterator<LocatedFileStatus> fileIterator;
    private FileSystem fileSystem;
    private PathFilter fileFilter;

    public boolean getHasNullValueInConditions() {
        return hasNullValueInConditions;
    }

    public List<String> getConditionFields() {
        return conditionFields;
    }

    public HDFSParquetSchemaReader(final Configuration conf, final Path path, final String finalNamePattern, final String user,
        final List<CustomWrapperFieldExpression> projectedFields, final CustomWrapperConditionHolder condition)
        throws IOException, InterruptedException {

        try {
            this.configuration = conf;
            this.outputPath = path;

            if (!UserGroupInformation.isSecurityEnabled()) {
                this.fileSystem = FileSystem.get(FileSystem.getDefaultUri(this.configuration), this.configuration, user);
            } else {
                this.fileSystem = FileSystem.get(this.configuration);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("FileSystem is: " + this.fileSystem.getUri());
                LOG.debug("Path is: " + path);
            }

            this.fileFilter = new FileFilter(finalNamePattern);

            initFileIterator();

            this.projectedFields = projectedFields;

            this.hasNullValueInConditions = false;
            this.conditionFields = condition != null ? this.getFieldsNameAndCheckNullConditions(condition.getComplexCondition()) : null;
        } catch (final IOException e) {
            if (this.fileSystem != null) {
                this.fileSystem.close();
            }

            throw e;
        } catch (final InterruptedException e) {
            if (this.fileSystem != null) {
                this.fileSystem.close();
            }

            throw e;
        } catch (final RuntimeException e) {
            if (this.fileSystem != null) {
                this.fileSystem.close();
            }

            throw e;
        }
    }

    public SchemaElement getSchema(final Configuration configuration) throws IOException {

        final Path filePath = nextFilePath();
        this.parquetSchema = getParquetSchema(configuration, filePath);

        final boolean isMandatory = this.parquetSchema.getRepetition() == Type.Repetition.REQUIRED  ? true : false;
        final SchemaElement schemaElement = new SchemaElement(this.parquetSchema.getName(), Object.class, isMandatory);

        return ParquetSchemaUtils.buildSchema(this.parquetSchema, schemaElement);

    }

    private MessageType getParquetSchema(final Configuration configuration, final Path filePath) throws IOException {
        ParquetReadOptions parquetReadOptions = ParquetReadOptions.builder().useSignedStringMinMax().useStatsFilter().useDictionaryFilter().useRecordFilter().build();
        ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath,configuration), parquetReadOptions);

        final ParquetMetadata readFooter = parquetFileReader.getFooter();
        final MessageType schema = readFooter.getFileMetaData().getSchema();

        return schema;
    }

    public MessageType getProjectedSchema() throws IOException {
        // Sets the expected schema so Parquet could validate that all files (e.g. in a directory) follow this schema.
        // If the schema is not set Parquet will use the schema contained in each Parquet file
        if (this.parquetSchema == null) {
            this.parquetSchema = getProjectedParquetSchema(configuration, currentPath);
        } else {
            this.schemaWithProjectedAndConditionFields(this.parquetSchema);
        }
        return this.parquetSchema;
    }

    private MessageType getProjectedParquetSchema(final Configuration configuration, final Path filePath) throws IOException {
        ParquetReadOptions parquetReadOptions = ParquetReadOptions.builder().useSignedStringMinMax().useStatsFilter().useDictionaryFilter().useRecordFilter().build();
        ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath,configuration), parquetReadOptions);

        final ParquetMetadata readFooter = parquetFileReader.getFooter();
        final MessageType schema = readFooter.getFileMetaData().getSchema();

        this.schemaWithProjectedAndConditionFields(schema);

        return schema;
    }

    private void schemaWithProjectedAndConditionFields(MessageType schema) {
        if (this.projectedFields != null || this.conditionFields != null) {
            List<Integer> schemaFieldsToDelete  = new ArrayList<Integer>();

            List<Type> schemaWithProjectionAndConditionFields = new ArrayList<Type>();
            for (CustomWrapperFieldExpression projectedField : this.projectedFields) {
                for (Type schemaField : schema.getFields()) {
                    if (schemaField.getName().equals(projectedField.getName())) {
                        schemaWithProjectionAndConditionFields.add(schemaField);
                    }
                }
            }
            for (String conditionField : this.conditionFields) {
                for (Type schemaField : schema.getFields()) {
                    if (schemaField.getName().equals(conditionField)) {
                        schemaWithProjectionAndConditionFields.add(schemaField);
                    }
                }
            }
            if (schema.getFields().size() != schemaWithProjectionAndConditionFields.size()) {
                schema.getFields().removeAll(schema.getFields());
                schema.getFields().addAll(schemaWithProjectionAndConditionFields);
            }
        }
    }

    public void initFileIterator() throws IOException {

        this.fileIterator = this.fileSystem.listFiles(this.outputPath, true);
        if (!this.fileIterator.hasNext()) {
            throw new IOException("'" + this.outputPath + "' does not exist or it denotes an empty directory");
        }
    }

    /**
     * Get the condition field names excluding the projected field names and compound fields (compound fields is not included in projections).
     * This method also initialize the hasNullValueInConditions variable
     *
     * @param condition
     * @return list with fields
     */
    private List<String> getFieldsNameAndCheckNullConditions(CustomWrapperCondition condition) throws IOException {
        List<String> conditionFields = new ArrayList<String>();
        if (condition != null) {
            if (condition.isAndCondition()) {
                CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) condition;
                for (CustomWrapperCondition c : andCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        //We only add fieldName to conditionFields if it is not a compound type and if it is not already included.
                        if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                            conditionFields.add(fieldName);
                        }
                        if (this.hasNullValueInConditions == false && hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) c)) {
                            this.hasNullValueInConditions = true;
                        }
                    } else {
                        List<String> fieldsName = this.getFieldsNameAndCheckNullConditions(c);
                        for (String fieldName : fieldsName) {
                            if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                                conditionFields.add(fieldName);
                            }
                        }
                    }
                }
            } else if (condition.isOrCondition()) {
                CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) condition;
                for (CustomWrapperCondition c : orCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                            conditionFields.add(fieldName);
                        }
                        if (this.hasNullValueInConditions == false && hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) c)) {
                            this.hasNullValueInConditions = true;
                        }
                    } else {
                        List<String> fieldsName = this.getFieldsNameAndCheckNullConditions(c);
                        for (String fieldName : fieldsName) {
                            if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                                conditionFields.add(fieldName);
                            }
                        }
                    }
                }
            } else if (condition.isSimpleCondition()) {
                String fieldName = ((CustomWrapperSimpleCondition) condition).getField().toString();
                if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                    conditionFields.add(fieldName);
                }
                if (this.hasNullValueInConditions == false && hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) condition)) {
                    this.hasNullValueInConditions = true;
                }
            } else {
                throw new IOException("Condition \"" + condition.toString() + "\" not allowed");
            }
        }
        for (CustomWrapperFieldExpression projectedField : this.projectedFields) {
            if(conditionFields.contains(projectedField.getName())) {
                conditionFields.remove(projectedField.getName());
            }
        }
        return  conditionFields;
    }

    /**
     * Get if a simple condition evaluate a null value
     *
     * @param vdpCondition
     * @return
     */
    private boolean hasNullValueInSimpleCondition(CustomWrapperSimpleCondition vdpCondition) {
        CustomWrapperSimpleCondition simpleCondition = vdpCondition;
        for (CustomWrapperExpression expression : simpleCondition.getRightExpression()) {
            if (expression.isSimpleExpression()) {
                CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression) expression;
                if (simpleExpression.getValue() == null) {
                    return true;
                }
            }
        }
        return false;
    }

    public Path nextFilePath() throws IOException {

        Path path = null;
        boolean found = false;
        while (this.fileIterator.hasNext() && !found) {

            final FileStatus fileStatus = this.fileIterator.next();

            if (this.fileFilter.accept(fileStatus.getPath())) {
                if (fileStatus.isFile()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Path of the file to read is: " + fileStatus.getPath());
                    }

                    path = fileStatus.getPath();
                } else if (fileStatus.isSymlink()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Path of the symbolic link to read is: " + fileStatus.getSymlink());
                    }

                    path = fileStatus.getSymlink();
                } else {
                    throw new IllegalArgumentException(
                        "'" + fileStatus.getPath() + "' is neither a file nor symbolic link");
                }
                found = true;
            }
        }

        if (path == null) {
            throw new NoSuchElementException();
        }
        this.currentPath = path;
        return path;
    }

    public void close() throws IOException {

        if (this.fileSystem != null) {
            this.fileSystem.close();
            this.fileSystem = null;
        }
    }


    public void delete() throws IOException {

        if (this.fileSystem == null) {
            this.fileSystem = FileSystem.get(this.configuration);
        }
        this.fileSystem.delete(this.outputPath, true);
        this.fileSystem.close();
        this.fileSystem = null;
    }

    @Override
    public boolean hasNext() {
        try {
            if (this.fileIterator.hasNext()) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            LOG.error("File Iterator error " + e.getMessage(), e);
            return  false;
        }
    }

    @Override
    public Object next() {
        Path path = null;
        boolean found = false;
        try {
            while (this.fileIterator.hasNext() && !found) {

                final FileStatus fileStatus = this.fileIterator.next();

                if (this.fileFilter.accept(fileStatus.getPath())) {
                    if (fileStatus.isFile()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Path of the file to read is: " + fileStatus.getPath());
                        }

                        path = fileStatus.getPath();
                    } else if (fileStatus.isSymlink()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Path of the symbolic link to read is: " + fileStatus.getSymlink());
                        }

                        path = fileStatus.getSymlink();
                    } else {
                        throw new IllegalArgumentException(
                            "'" + fileStatus.getPath() + "' is neither a file nor symbolic link");
                    }
                    found = true;
                }
            }
            this.currentPath = path;
        } catch (IOException e) {
            LOG.error("File Iterator error " + e.getMessage(), e);
            return  null;
        }
        return path;
    }
}
