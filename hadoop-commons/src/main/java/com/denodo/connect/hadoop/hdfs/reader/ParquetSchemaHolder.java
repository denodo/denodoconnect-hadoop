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

import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.buildMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.io.ConditionUtils;
import com.denodo.connect.hadoop.hdfs.util.io.PartitionUtils;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaUtils;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;

public class ParquetSchemaHolder {

    private MessageType fileSchema;
    private MessageType partitionSchema;
    private MessageType querySchema;
    private List<BlockMetaData> rowGroups;
    private ParquetMetadata footer;
    private List<CustomWrapperFieldExpression> projectedFields;
    private Collection<String> simpleConditions;


    public ParquetSchemaHolder(final Configuration conf, final Path path, final List<CustomWrapperFieldExpression> projectedFields,
        final CustomWrapperConditionHolder condition) throws IOException {

        this.projectedFields = projectedFields != null ? projectedFields : new ArrayList<CustomWrapperFieldExpression>(0);
        this.simpleConditions = condition != null ? ConditionUtils.getSimpleConditionFields(condition.getComplexCondition()):
            new ArrayList<String>(0);

        readMetadata(conf, path);
    }

    public MessageType getFileSchema() {
        return this.fileSchema;
    }

    public SchemaElement getWrapperSchema() {

        final boolean isMandatory = this.fileSchema.getRepetition() == REQUIRED;
        final SchemaElement schemaElement = new SchemaElement(this.fileSchema.getName(), Object.class, isMandatory);

        final MessageType wrapperSchema =  this.partitionSchema != null ? this.fileSchema.union(this.partitionSchema, false) : this.fileSchema;

        return ParquetSchemaUtils.buildSchema(wrapperSchema, schemaElement);
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

    public Collection<String> getConditionFields() {
        return this.simpleConditions;
    }

    public List<String> getPartitionFields() {

        final List<String> partitionFields = new ArrayList<>();

        if (this.partitionSchema != null) {
            final List<Type> partitionTypes = this.partitionSchema.getFields();
            if (!partitionTypes.isEmpty() && !this.simpleConditions.isEmpty()) {
                for (final Type partitionType : partitionTypes) {
                    if (this.simpleConditions.contains(partitionType.getName())) {
                        partitionFields.add(partitionType.getName());
                    }
                }
            }
        }

        return partitionFields;
    }

    private void readMetadata(final Configuration configuration, final Path filePath) throws IOException {

        try (final ParquetFileReader parquetFileReader = ParquetFileReader.open(configuration, filePath)) {

            this.footer = parquetFileReader.getFooter();
            this.rowGroups = parquetFileReader.getRowGroups();
            this.fileSchema = this.footer.getFileMetaData().getSchema();
            this.partitionSchema = getPartitionMetadata(filePath);
            this.querySchema = schemaWithProjectedAndConditionFields(this.fileSchema);
        }
    }

    private static MessageType getPartitionMetadata(final Path path) {

        MessageType partitionSchema = null;

        final List<Type> partitionFields = PartitionUtils.getPartitionFields(path);
        if (!partitionFields.isEmpty()) {
            partitionSchema = new MessageType("PartitionSchema", partitionFields);
        }

        return partitionSchema;
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

        for (final String conditionField : excludeProjectedFields(this.simpleConditions, this.projectedFields)) {
            try {
                final Type schemaField = schema.getType(conditionField);
                newSchema.addField(schemaField);
            } catch (final InvalidRecordException e) {
                // ignore
            }
        }

        return newSchema.named("schema");
    }

    private static List<String> excludeProjectedFields(final Collection<String> conditionFields,
        final List<CustomWrapperFieldExpression> projectedFields) {

            final List<String> conditionsExcludingProjectedFields = new ArrayList<>(conditionFields);

            if (! conditionFields.isEmpty()) {
                for (final CustomWrapperFieldExpression projectedField : projectedFields) {
                    conditionsExcludingProjectedFields.remove(projectedField.getName());
                }
            }

        return conditionsExcludingProjectedFields;
    }

    public void updateCondition(final CustomWrapperConditionHolder condition) {

        this.simpleConditions = condition != null ? ConditionUtils.getSimpleConditionFields(condition.getComplexCondition())
            : new ArrayList<String>(0);
        this.querySchema = schemaWithProjectedAndConditionFields(this.fileSchema);

    }
}
