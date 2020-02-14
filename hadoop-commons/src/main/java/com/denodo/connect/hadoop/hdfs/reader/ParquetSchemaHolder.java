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
package com.denodo.connect.hadoop.hdfs.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaUtils;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class ParquetSchemaHolder {


    private MessageType fileSchema;
    private MessageType querySchema;
    private List<BlockMetaData> rowGroups;
    private ParquetMetadata footer;
    private List<CustomWrapperFieldExpression> projectedFields;
    private List<String> conditionFields;
    private List<String> conditionExcludingProjectedFields;


    public ParquetSchemaHolder(final Configuration conf, final Path path, final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperConditionHolder condition) throws IOException {

        this.projectedFields = projectedFields != null ? projectedFields : Collections.emptyList();
        this.conditionFields = condition != null ? getConditionFields(condition.getComplexCondition()): Collections.emptyList();
        this.conditionExcludingProjectedFields = excludeProjectedFields(this.conditionFields, projectedFields);

        readMetadata(conf, path);
    }

    public MessageType getFileSchema() {
        return this.fileSchema;
    }

    public SchemaElement getWrapperSchema() {

        final boolean isMandatory = this.fileSchema.getRepetition() == Type.Repetition.REQUIRED;
        final SchemaElement schemaElement = new SchemaElement(this.fileSchema.getName(), Object.class, isMandatory);

        return ParquetSchemaUtils.buildSchema(this.fileSchema, schemaElement);
    }

    public MessageType getQuerySchema() {
        return this.querySchema;
    }

    public List<BlockMetaData> getRowGroups() {
        return this.rowGroups;
    }

    public ParquetMetadata getFooter() {
        return this.footer;
    }

    public List<String> getConditionFields() {
        return this.conditionFields;
    }

    public List<String> getConditionExcludingProjectedFields() {
        return this.conditionExcludingProjectedFields;
    }

    private void readMetadata(final Configuration configuration, final Path filePath) throws IOException {

        try (final ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration))) {

            this.footer = parquetFileReader.getFooter();
            this.rowGroups = parquetFileReader.getRowGroups();
            this.fileSchema = this.footer.getFileMetaData().getSchema();
            this.querySchema = schemaWithProjectedAndConditionFields(this.fileSchema);
        }
    }

    private MessageType schemaWithProjectedAndConditionFields(final MessageType schema) {

        final Types.MessageTypeBuilder newSchema = buildMessage();

        for (final CustomWrapperFieldExpression projectedField : this.projectedFields) {
            try {
                final Type schemaField = schema.getType(projectedField.getName());
                newSchema.addField(schemaField);
            } catch (final InvalidRecordException e) {
                // ignore
            }
        }

        for (final String conditionField : this.conditionExcludingProjectedFields) {
            try {
                final Type schemaField = schema.getType(conditionField);
                newSchema.addField(schemaField);
            } catch (final InvalidRecordException e) {
                // ignore
            }
        }

        return newSchema.named("schema");
    }

    /**
     * Get the condition field names excluding compound fields (compound fields are not included in projections).
     */
    private List<String> getConditionFields(final CustomWrapperCondition condition) throws IOException {

        final List<String> conditionFields = new ArrayList<>();
        if (condition != null) {
            if (condition.isAndCondition()) {
                final CustomWrapperAndCondition andCondition = (CustomWrapperAndCondition) condition;
                for (final CustomWrapperCondition c : andCondition.getConditions()) {
                    if (c.isSimpleCondition()) {
                        final String fieldName = ((CustomWrapperSimpleCondition) c).getField().toString();
                        //We only add fieldName to conditionFields if it is not a compound type and if it is not already included.
                        if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1) {
                            conditionFields.add(fieldName);
                        }

                    } else {
                        final List<String> fieldsName = this.getConditionFields(c);
                        for (final String fieldName : fieldsName) {
                            if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1) {
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

                    } else {
                        final List<String> fieldsName = this.getConditionFields(c);
                        for (final String fieldName : fieldsName) {
                            if (!conditionFields.contains(fieldName) && fieldName.split("\\.").length == 1) {
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

            } else {
                throw new IOException("Condition \"" + condition.toString() + "\" not allowed");
            }
        }

        return  conditionFields;
    }

    private static List<String> excludeProjectedFields(final List<String> conditionFields,
        final List<CustomWrapperFieldExpression> projectedFields) {

            final List<String> conditionsExcludingProjectedFields = new ArrayList<>(conditionFields);

            if (! conditionFields.isEmpty()) {
                for (final CustomWrapperFieldExpression projectedField : projectedFields) {
                    conditionsExcludingProjectedFields.remove(projectedField.getName());
                }
            }

        return conditionsExcludingProjectedFields;
    }
}
