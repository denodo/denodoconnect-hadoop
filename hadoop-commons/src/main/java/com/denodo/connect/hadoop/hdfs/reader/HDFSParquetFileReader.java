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

import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_EQ;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_GT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LE;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_LT;
import static com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition.OPERATOR_NE;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import com.denodo.connect.hadoop.hdfs.util.io.FileFilter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
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
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperAndCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperConditionHolder;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperOrCondition;
import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperSimpleCondition;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperSimpleExpression;


public class HDFSParquetFileReader implements HDFSFileReader {

    private static final  Logger LOG = LoggerFactory.getLogger(HDFSParquetFileReader.class);

    private ParquetReader<Group> dataFileReader;
    private List<CustomWrapperFieldExpression> projectedFields;
    private MessageType parquetSchema;
    private FilterCompat.Filter filter;
    private List<String> conditionFields;

    private Configuration configuration;

    private FileSystem fileSystem;
    private RemoteIterator<LocatedFileStatus> fileIterator;
    private boolean firstReading;
    private Path path;
    private boolean includePathColumn;

    public HDFSParquetFileReader(final Configuration conf, final Path path, final String user,
    		final List<CustomWrapperFieldExpression> projectedFields, final boolean includePathColumn,
            FilterCompat.Filter filter, MessageType parquetSchema, List<String> conditionFields)
            throws IOException, InterruptedException {

        try {
            this.configuration = conf;
            this.path = path;

            if (!UserGroupInformation.isSecurityEnabled()) {
                this.fileSystem = FileSystem.get(FileSystem.getDefaultUri(this.configuration), this.configuration,
                    user);
            } else {
                this.fileSystem = FileSystem.get(this.configuration);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("FileSystem is: " + this.fileSystem.getUri());
                LOG.debug("Path is: " + path);
            }

            initFileIterator();

            this.firstReading = true;
            this.includePathColumn = includePathColumn;
            this.parquetSchema = parquetSchema;

            this.projectedFields = projectedFields;
            this.filter = filter;
            this.conditionFields = conditionFields;
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

    public void doOpenReader(final Path path, final Configuration configuration) throws IOException {

        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, this.parquetSchema.toString());
        
        final GroupReadSupport groupReadSupport = new GroupReadSupport();
        if (filter != null) {
            this.dataFileReader = ParquetReader.builder(groupReadSupport, path).withConf(configuration).withFilter(this.filter).build();
        } else {
            this.dataFileReader = ParquetReader.builder(groupReadSupport, path).withConf(configuration).build();
        }
    }

    public Object doRead() throws IOException {
        
        final Group data = this.dataFileReader.read();
        if (data != null) {
            return readParquetLogicalTypes(data, this.projectedFields, this.conditionFields);
        }

        return null;

    }

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
     * Method for read all the parquet types excluding the conditionFields
     * @return Record with the fields
     */
    private static Object[] readParquetLogicalTypes(final Group datum, final List<CustomWrapperFieldExpression> projectedFields, List<String> conditionFields)
        throws IOException {
        final List<Type> fields = datum.getType().getFields();
        final Object[] vdpRecord = new Object[fields.size() - conditionFields.size()];
        int i = 0;
        for (final Type field : fields) {
            if (!conditionFields.contains(field.getName()))  {
                vdpRecord[i] = readTypes(datum, projectedFields, field);
            }
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
                        // We need to add one day because the index start in 0.
                        final long daysMillis = TimeUnit.DAYS.toMillis(days+1);
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

    public void initFileIterator() throws IOException {

        this.fileIterator = this.fileSystem.listFiles(this.path, true);
        if (!this.fileIterator.hasNext()) {
            throw new IOException("'" + this.path + "' does not exist or it denotes an empty directory");
        }
    }

    public void openReader(final FileSystem fs, final Configuration conf) throws IOException {

        try {
            doOpenReader(this.path, conf);

        } catch (final NoSuchElementException e) {
            throw e;
        } catch (final IOException e) {
            throw new IOException("'" + this.path + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        } catch (final RuntimeException e) {
            throw new RuntimeException("'" + this.path + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        }
    }

    private boolean isFirstReading() {
        return this.firstReading;
    }

    @Override
    public Object read() throws IOException {

        try {

            if (isFirstReading()) {
                LOG.error(Thread.currentThread() + " DEBUG1 firstReading");
                openReader(this.fileSystem, this.configuration);
                this.firstReading = false;
            }

            Object data = doRead();
            if (data != null) {
                if(this.includePathColumn){
                    data= ArrayUtils.add((Object[])data, this.path.toString());
                }
                return data;
            }

            // This reader does not have anything read -> take next one
            closeReader();

            close();
            return null;

        } catch (final NoSuchElementException e) {
            return null;
        } catch (final IOException e) {
            throw new IOException("'" + this.path + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        } catch (final RuntimeException e) {
            throw new RuntimeException("'" + this.path + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        }

    }

    @Override
    public void close() throws IOException {
        closeReader();
        if (this.fileSystem != null) {
            this.fileSystem.close();
            this.fileSystem = null;
        }
    }

    @Override
    public void delete() throws IOException {

        if (this.fileSystem == null) {
            this.fileSystem = FileSystem.get(this.configuration);
        }
        this.fileSystem.delete(this.path, true);
        this.fileSystem.close();
        this.fileSystem = null;
    }
    
}
