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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;

public class ParquetSchemaBuilder {


    private List<CustomWrapperFieldExpression> projectedFields;
    private MessageType parquetSchema;
    private boolean hasNullValueInConditions;
    private List<String> conditionFields;
    private List<String> conditionsIncludingProjectedFields;

    private Configuration configuration;

    private Path path;
    private List<BlockMetaData> rowGroups;
    private ParquetMetadata footer;


    public ParquetSchemaBuilder(final Configuration conf, final Path path, final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperConditionHolder condition) throws IOException {

        this.configuration = conf;
        this.path = path;

        this.projectedFields = projectedFields;

        this.hasNullValueInConditions = false;
        this.conditionFields = condition != null ? this.getFieldsNameAndCheckNullConditions(condition.getComplexCondition()) : null;

    }

    public boolean hasNullValueInConditions() {
        return this.hasNullValueInConditions;
    }

    public List<String> getConditionFields() {
        return this.conditionFields;
    }

    public List<String> getConditionsIncludingProjectedFields() {
        return this.conditionsIncludingProjectedFields;
    }

    public List<BlockMetaData> getRowGroups() {
        return this.rowGroups;
    }

    public ParquetMetadata getFooter() {
        return this.footer;
    }

    public SchemaElement getSchema() throws IOException {

        this.parquetSchema = getParquetSchema(this.configuration, this.path);

        final boolean isMandatory = this.parquetSchema.getRepetition() == Type.Repetition.REQUIRED;
        final SchemaElement schemaElement = new SchemaElement(this.parquetSchema.getName(), Object.class, isMandatory);

        return ParquetSchemaUtils.buildSchema(this.parquetSchema, schemaElement);

    }

    private MessageType getParquetSchema(final Configuration configuration, final Path filePath) throws IOException {

        MessageType schema;
        try (final ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration))) {

            this.footer = parquetFileReader.getFooter();
            this.rowGroups = parquetFileReader.getRowGroups();
            schema = this.footer.getFileMetaData().getSchema();
        }

        return schema;
    }

    public MessageType getFileSchema() throws IOException {

        if (this.parquetSchema == null) {
            this.parquetSchema = getParquetSchema(this.configuration, this.path);
        }

        return this.parquetSchema;
    }

    public MessageType getProjectedSchema() throws IOException {
        // Sets the expected schema so Parquet could validate that all files (e.g. in a directory) follow this schema.
        // If the schema is not set Parquet will use the schema contained in each Parquet file
        if (this.parquetSchema == null) {
            this.parquetSchema = getParquetSchema(this.configuration, this.path);
        }

        return schemaWithProjectedAndConditionFields(this.parquetSchema);
    }

    private MessageType  schemaWithProjectedAndConditionFields(final MessageType schema) {

        final Types.MessageTypeBuilder newSchema = Types.buildMessage();
        if (this.projectedFields != null || this.conditionFields != null) {

            for (final CustomWrapperFieldExpression projectedField : this.projectedFields) {
                for (final Type schemaField : schema.getFields()) {
                    if (schemaField.getName().equals(projectedField.getName())) {
                        newSchema.addField(schemaField);
                    }
                }
            }
            for (final String conditionField : this.conditionFields) {
                for (final Type schemaField : schema.getFields()) {
                    if (schemaField.getName().equals(conditionField)) {
                        newSchema.addField(schemaField);
                    }
                }
            }
        }
        return newSchema.named("schema");
    }

    /**
     * Get the condition field names excluding the projected field names and compound fields (compound fields is not included in projections).
     * This method also initialize the hasNullValueInConditions variable
     */
    private List<String> getFieldsNameAndCheckNullConditions(final CustomWrapperCondition condition) throws IOException {

        final List<String> conditionFields = new ArrayList<>();
        if (condition != null) {
            if (condition.isAndCondition()) {
                final CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) condition;
                for (final CustomWrapperCondition c : andCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        final String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        //We only add fieldName to conditionFields if it is not a compound type and if it is not already included.
                        if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                            conditionFields.add(fieldName);
                        }
                        if (!this.hasNullValueInConditions && hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) c)) {
                            this.hasNullValueInConditions = true;
                        }
                    } else {
                        final List<String> fieldsName = this.getFieldsNameAndCheckNullConditions(c);
                        for (final String fieldName : fieldsName) {
                            if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                                conditionFields.add(fieldName);
                            }
                        }
                    }
                }
            } else if (condition.isOrCondition()) {
                final CustomWrapperOrCondition orCondition = (CustomWrapperOrCondition) condition;
                for (final CustomWrapperCondition c : orCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        final String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                            conditionFields.add(fieldName);
                        }
                        if (!this.hasNullValueInConditions && hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) c)) {
                            this.hasNullValueInConditions = true;
                        }
                    } else {
                        final List<String> fieldsName = this.getFieldsNameAndCheckNullConditions(c);
                        for (final String fieldName : fieldsName) {
                            if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1){
                                conditionFields.add(fieldName);
                            }
                        }
                    }
                }
            } else if (condition.isSimpleCondition()) {
                final String fieldName = ((CustomWrapperSimpleCondition) condition).getField().toString();
                if (fieldName.split("\\.").length == 1){
                    conditionFields.add(fieldName);
                }
                if (!this.hasNullValueInConditions && hasNullValueInSimpleCondition((CustomWrapperSimpleCondition) condition)) {
                    this.hasNullValueInConditions = true;
                }
            } else {
                throw new IOException("Condition \"" + condition.toString() + "\" not allowed");
            }
        }

        this.conditionsIncludingProjectedFields = new ArrayList<>(conditionFields);

        for (final CustomWrapperFieldExpression projectedField : this.projectedFields) {
            conditionFields.remove(projectedField.getName());
        }
        return  conditionFields;
    }

    /**
     * Get if a simple condition evaluate a null value
     *
     */
    private boolean hasNullValueInSimpleCondition(final CustomWrapperSimpleCondition vdpCondition) {

        for (final CustomWrapperExpression expression : vdpCondition.getRightExpression()) {
            if (expression.isSimpleExpression()) {
                final CustomWrapperSimpleExpression simpleExpression = (CustomWrapperSimpleExpression) expression;
                if (simpleExpression.getValue() == null) {
                    return true;
                }
            }
        }
        return false;
    }

}
