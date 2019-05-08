/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2013, denodo technologies (http://www.denodo.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * =============================================================================
 */
package com.denodo.connect.hadoop.hdfs.reader;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaUtils;
import com.denodo.connect.hadoop.hdfs.util.type.ParquetTypeUtils;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;


public class HDFSParquetFileReader extends AbstractHDFSFileReader {

    private static final  Logger LOG = LoggerFactory.getLogger(HDFSParquetFileReader.class);

    private ParquetReader<Group> dataFileReader;
    private List<CustomWrapperFieldExpression> projectedFields;
    private MessageType parquetSchema;
    

    public HDFSParquetFileReader(final Configuration conf, final Path path, final String finalNamePattern,
            final String user, final List<CustomWrapperFieldExpression> projectedFields) 
            throws IOException, InterruptedException {

        super(conf, path, finalNamePattern, user);
        this.projectedFields = projectedFields;
    }

    @Override
    public void doOpenReader(final FileSystem fileSystem, final Path path,
            final Configuration configuration) throws IOException {
        
        if (this.parquetSchema == null) {
            this.parquetSchema = getParquetSchema(configuration, path);
        }
        
        // Sets the expected schema so Parquet could validate that all files (e.g. in a directory) follow this schema. 
        // If the schema is not set Parquet will use the schema contained in each Parquet file 
        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, this.parquetSchema.toString());
        
        final GroupReadSupport groupReadSupport = new GroupReadSupport();
        this.dataFileReader = ParquetReader.builder(groupReadSupport, path).withConf(configuration).build();
    }

    public SchemaElement getSchema(final Configuration configuration) throws IOException {

        final Path filePath = nextFilePath();
        this.parquetSchema = getParquetSchema(configuration, filePath);
        
        final boolean isMandatory = this.parquetSchema.getRepetition() == Repetition.REQUIRED  ? true : false;
        final SchemaElement schemaElement = new SchemaElement(this.parquetSchema.getName(), Object.class, isMandatory); 

        return ParquetSchemaUtils.buildSchema(this.parquetSchema, schemaElement);

    }

    private MessageType getParquetSchema(final Configuration configuration, final Path filePath) throws IOException {
        
        final ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, filePath, ParquetMetadataConverter.NO_FILTER);
        
        final MessageType schema = readFooter.getFileMetaData().getSchema();
        
        return schema;
    }

    @Override
    public Object doRead() throws IOException {
        
        final Group data = this.dataFileReader.read();
        if (data != null) {
            return readParquetLogicalTypes(data, this.projectedFields);
        }

        return null;

    }

    @Override
    public void closeReader() throws IOException {
        if (this.dataFileReader != null) {
            this.dataFileReader.close();
        }
    }

    /**
     * Method for read all the parquet types
     * @return Record with the fields
     */
    private static Object[] readParquetLogicalTypes(final Group datum, final List<CustomWrapperFieldExpression> projectedFields)
            throws IOException {
        final List<Type> fields = datum.getType().getFields();
        final Object[] vdpRecord = new Object[fields.size()];
        int i = 0;
        for (final Type field : fields) {
            vdpRecord[i] = readTypes(datum, projectedFields, field);
            i++;
        }
        return vdpRecord;
    }

    /**
     * Method for read parquet types. The method does the validation to know what type needs to be read.
     */
    private static Object readTypes(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, final Type valueList)
            throws IOException {
        
        if (valueList.isPrimitive()) {
            return readPrimitive(datum, valueList);
        }
        
        if (ParquetTypeUtils.isGroup(valueList)) {
            return readGroup(datum, projectedFields, valueList);
        } else if (ParquetTypeUtils.isMap(valueList)) {
            return readMap(datum, projectedFields, valueList);
        } else if (ParquetTypeUtils.isList(valueList)) {
            return readList(datum, projectedFields, valueList);
        } else {
            LOG.error("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");
            throw new IOException("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");
        }
    }

    /**
     * Read method for parquet primitive types
     */
    private static Object readPrimitive(final Group datum, final Type field) throws IOException {
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            final PrimitiveTypeName   primitiveTypeName = field.asPrimitiveType().getPrimitiveTypeName();
            try{
                if(primitiveTypeName.equals(PrimitiveTypeName.BINARY)) {
                    if(field.getOriginalType()!=null){
                        if(field.getOriginalType().equals(OriginalType.UTF8)){
                            return datum.getString(field.getName(), 0);
                        } else if(field.getOriginalType().equals(OriginalType.JSON)){
                            return datum.getString(field.getName(), 0);
                        } else if(field.getOriginalType().equals(OriginalType.BSON)){
                            return datum.getString(field.getName(), 0);
                        } else{
                            return datum.getBinary(field.getName(), 0).getBytes();
                        }
                    }
                    return datum.getBinary(field.getName(), 0).getBytes();
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.BOOLEAN)) {
                    return  datum.getBoolean(field.getName(), 0);
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.DOUBLE)) {
                    return datum.getDouble(field.getName(), 0);
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.FLOAT)) {
                    return datum.getFloat(field.getName(), 0);
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.INT32)) {
                    if (OriginalType.DECIMAL.equals(field.getOriginalType())) {
                        final int scale = field.asPrimitiveType().getDecimalMetadata().getScale();
                        return new BigDecimal(BigInteger.valueOf(datum.getInteger(field.getName(), 0)), scale);
                    } else if (OriginalType.DATE.equals(field.getOriginalType())) {
                        //   DATE fields really holds the number of days since 1970-01-01
                        final int days = datum.getInteger(field.getName(),0);
                        final long daysMillis = TimeUnit.DAYS.toMillis(days); 
                        return new Date(daysMillis);
                    } else {
                        return datum.getInteger(field.getName(), 0);
                    }
                    
                } else if(primitiveTypeName.equals(PrimitiveTypeName.INT64)) {
                    //we dont differentiate INT64 from TIMESTAMP_MILLIS original types
                    return datum.getLong(field.getName(), 0);
                    
                } else if(primitiveTypeName.equals(PrimitiveTypeName.INT96)) {
                    Binary binary = datum.getInt96(field.getName(), 0);
                    long timestampMillis = ParquetTypeUtils.int96ToTimestampMillis(binary);
                    return new Date(timestampMillis);
                    
                } else if(primitiveTypeName.equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                    if (OriginalType.DECIMAL.equals(field.getOriginalType())) {
                        final int scale = field.asPrimitiveType().getDecimalMetadata().getScale();
                        return new BigDecimal(new BigInteger(datum.getBinary(field.getName(), 0).getBytes()), scale);
                    }
                    return datum.getBinary(field.getName(), 0).getBytes();
                } else{
                    LOG.error("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                    throw new IOException("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                }
            } catch(final RuntimeException e){
                LOG.warn("It was a error reading data", e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        return null;
    }
    
    /**
     * Read method for parquet group types
     * @return Record with the fields
     */
    private static Object readGroup(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, final Type field) throws IOException {
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            Object[] vdpGroupRecord = new Object[field.asGroupType().getFields().size()];
            vdpGroupRecord = readParquetLogicalTypes(datum.getGroup(field.getName(), 0), projectedFields);
            return vdpGroupRecord;
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        LOG.trace("The group " + field.getName() + " doesn't exist in the data");
        return null;
    }
    
    /**
     * Read method for parquet list types
     * @return Array with the fields
     */
    private static Object readList(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, final Type field) throws IOException {
        //This only works with the last parquet version 1.5.0 Previous versions maybe don't have a valid format.
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    final Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][1];
                    for (int j = 0; j < datumGroup.getFieldRepetitionCount(0); j++) {
                        final Group datumList = datumGroup.getGroup(0, j)!= null ? datumGroup.getGroup(0, j) : null;
                        if (datumList != null && datumList.getType().getFields().size() > 0) {
                            vdpArrayRecord[j][0] = readTypes(datumList, projectedFields, datumList.getType().getFields().get(0));
                        } else {
                            throw new IOException("The list element " + field.getName() + " don't have a valid format");
                        }
                    }
                    return vdpArrayRecord;
                }
            } catch (final ClassCastException e) {
                throw new IOException("The list element " + field.getName() + " don't have a valid format",e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        LOG.trace("The list " + field.getName() + " doesn't exist in the data");
        return null;
    }
    
    /**
     * Read method for parquet map types
     * @return Array with the fields
     */
    private static Object readMap(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, final Type field) throws IOException {
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    final Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][2];
                    for (int j = 0; j < datumGroup.getFieldRepetitionCount(0); j++) {
                        final Group datumMap = datumGroup.getGroup(0, j) != null ? datumGroup.getGroup(0, j) : null;
                        if (datumMap != null && datumMap.getType().getFields().size() > 1) {
                            vdpArrayRecord[j][0] = readPrimitive(datumMap,datumMap.getType().getFields().get(0));
                            vdpArrayRecord[j][1] = readTypes(datumMap, projectedFields, datumMap.getType().getFields().get(0));
                        } else {
                            throw new IOException("The list element " + field.getName() + " don't have a valid format");
                        }
                    }
                    return vdpArrayRecord;
                }
            } catch (final ClassCastException e) {
                throw new IOException("The map element " + field.getName() + " don't have a valid format",e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        LOG.trace("The map " + field.getName() + " doesn't exist in the data");
        return null;
    }
    
}