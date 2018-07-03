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
import org.apache.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaUtils;
import com.denodo.connect.hadoop.hdfs.util.type.ParquetTypeUtils;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;


public class HDFSParquetFileReader extends AbstractHDFSFileReader {


    private ParquetReader<Group> dataFileReader;
    private List<CustomWrapperFieldExpression> projectedFields;
    private static final Logger logger = Logger.getLogger(HDFSParquetFileReader.class);

    public HDFSParquetFileReader(final Configuration conf, final Path path, 
            final String user, final List<CustomWrapperFieldExpression> projectedFields) throws IOException, InterruptedException {

        super(conf, path, user);
        this.projectedFields = projectedFields;
    }

    @Override
    public void openReader(final FileSystem fileSystem, final Path path,
            final Configuration configuration) throws IOException {
        final GroupReadSupport groupReadSupport=new GroupReadSupport();
        this.dataFileReader=ParquetReader.builder(groupReadSupport,path).withConf(configuration).build();
        if(this.dataFileReader==null){
            throw new IllegalArgumentException("'" + path + "' is not a parquet file, or there is other problem in the connection"); 
        }
    }

    public   SchemaElement getSchema(final FileSystem fileSystem, final Path path,
            final Configuration configuration) throws IOException, CustomWrapperException  {

        openReader(fileSystem, path, configuration);
        final Group group =  this.dataFileReader.read();
        boolean isMandatory = group.getType().getRepetition() == Repetition.REQUIRED  ? true : false;
        SchemaElement schemaElement = new SchemaElement(group.getType().getName(), Object.class, isMandatory); 
        final SchemaElement finalSchemaElement = ParquetSchemaUtils.buildSchema(group.getType(), schemaElement);
        closeReader();

        return finalSchemaElement;
    }

    @Override
    public Object doRead() throws IOException {

        final Group data = this.dataFileReader.read();
        if (data!=null) {
            return readParquetLogicalTypes( data, this.projectedFields);
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
     * @param datum 
     * @param projectedFields
     * @return Record with the fields
     * @throws IOException
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
     * @param datum
     * @param projectedFields
     * @param valueList
     * @return
     * @throws IOException
     */
    private static Object readTypes(Group datum, List<CustomWrapperFieldExpression> projectedFields, final Type valueList)
            throws IOException {
        if (valueList.isPrimitive()) {
            return readPrimitive(datum, valueList);
        } else {
            try {
                if (ParquetTypeUtils.isGroup(valueList)) {
                    return readGroup(datum, projectedFields, valueList);
                } else if (ParquetTypeUtils.isMap(valueList)) {
                    return readMap(datum, projectedFields, valueList);
                } else if (ParquetTypeUtils.isList(valueList)) {
                    return readList(datum, projectedFields, valueList);
                } else {
                    logger.error("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");
                    throw new IOException("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");
                }
            } catch (CustomWrapperException e) {
                throw new IOException("ERROR When try to convert to GroupType", e);
            }
        }
    }

    /**
     * Read method for parquet primitive types
     * @param datum
     * @param field
     * @throws IOException
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
                    }else{
                        return datum.getBinary(field.getName(), 0).getBytes();   
                    }
                    
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
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.INT64)) {
                    //we dont differentiate INT64 from TIMESTAMP_MILLIS original types
                    return datum.getLong(field.getName(), 0);
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.INT96)) {
                    return datum.getInt96(field.getName(), 0);
                    
                }else if(primitiveTypeName.equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                    
                    return datum.getBinary(field.getName(), 0).getBytes();
                }else{
                    logger.error("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                    throw new IOException("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                }
            }catch(final RuntimeException e){
                logger.warn("It was a error reading data", e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        return null;
    }
    
    /**
     * Read method for parquet group types
     * @param datum
     * @param projectedFields
     * @param field
     * @return Record with the fields
     * @throws IOException
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
        logger.trace("The group " + field.getName() + " doesn't exist in the data");
        return null;
    }
    
    /**
     * Read method for parquet list types
     * @param datum
     * @param projectedFields
     * @param field
     * @return Array with the fields
     * @throws IOException
     */
    private static Object readList(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, final Type field) throws IOException {
        //This only works with the last parquet version 1.5.0 Previous versions maybe don't have a valid format.
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][1];
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
            } catch (ClassCastException e) {
                throw new IOException("The list element " + field.getName() + " don't have a valid format",e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        logger.trace("The list " + field.getName() + " doesn't exist in the data");
        return null;
    }
    
    /**
     * Read method for parquet map types
     * @param datum
     * @param projectedFields
     * @param field
     * @return Array with the fields
     * @throws IOException
     */
    private static Object readMap(Group datum, List<CustomWrapperFieldExpression> projectedFields, Type field) throws IOException {
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][2];
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
            } catch (ClassCastException e) {
                throw new IOException("The map element " + field.getName() + " don't have a valid format",e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field "+ field.toString()+" is Required");
        }
        logger.trace("The map " + field.getName() + " doesn't exist in the data");
        return null;
    }
    
}
