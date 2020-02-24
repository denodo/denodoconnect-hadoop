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

import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.connect.hadoop.hdfs.util.type.ParquetTypeUtils;


public class HDFSParquetFileReader implements HDFSFileReader {

    private static final  Logger LOG = LoggerFactory.getLogger(HDFSParquetFileReader.class);

    private ParquetReader<Group> dataFileReader;
    private List<String> conditionFields;

    private String pathValue;
    private boolean includePathValue;

    private Long startingPos;
    private Long endingPos;

    public HDFSParquetFileReader(final Configuration conf, final Path path,  final boolean includePathValue,
        final FilterCompat.Filter filter, final MessageType parquetSchema, final List<String> conditionFields) throws IOException {

        this(conf, path, includePathValue, filter, parquetSchema, conditionFields, null, null);
    }

    public HDFSParquetFileReader(final Configuration conf, final Path path,  final boolean includePathValue,
        final FilterCompat.Filter filter, final MessageType parquetSchema, final List<String> conditionFields,
        final Long startingPos, final Long endingPos) throws IOException {

        this.pathValue = path.toString();
        this.includePathValue = includePathValue;
        this.conditionFields = conditionFields;
        this.startingPos = startingPos;
        this.endingPos = endingPos;

        openReader(path, conf, filter, parquetSchema);

    }

    private void openReader(final Path path, final Configuration configuration, final FilterCompat.Filter filter,
        final MessageType parquetSchema) throws IOException {

        try {
            configuration.set(ReadSupport.PARQUET_READ_SCHEMA, parquetSchema.toString());

            final GroupReadSupport groupReadSupport = new GroupReadSupport();
            final ParquetReader.Builder<Group> dataFileBuilder = ParquetReader.builder(groupReadSupport, path).withConf(configuration);

            if (filter != null) {
                dataFileBuilder.withFilter(filter);
            } if (this.startingPos != null && this.endingPos != null) {
                dataFileBuilder.withFileRange(this.startingPos, this.endingPos);
            }

            this.dataFileReader = dataFileBuilder.build();
        } catch (final IOException e) {
            throw new IOException("'" + path + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        } catch (final RuntimeException e) {
            throw new RuntimeException("'" + path + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        }

    }

    @Override
    public Object read() throws IOException {

        try {

            Object data = doRead();
            if (data != null) {
                if (this.includePathValue){
                    data = ArrayUtils.add((Object[]) data, this.pathValue);
                }
                return data;
            }

            close();
            return null;

        } catch (final IOException e) {
            close();
            throw new IOException('\'' + this.pathValue + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        } catch (final RuntimeException e) {
            close();
            throw new RuntimeException('\'' + this.pathValue + "': " + e.getMessage(), e); // Add the file name causing the error for an user friendly exception message
        }

    }

    private Object doRead() throws IOException {
        
        final Group data = this.dataFileReader.read();
        if (data != null) {
            return readParquetLogicalTypes(data, this.conditionFields);
        }

        return null;

    }

    @Override
    public void close() throws IOException {
        if (this.dataFileReader != null) {
            this.dataFileReader.close();
        }
    }

    @Override
    public void delete() throws IOException {

    }

    /**
     * Method for read all the parquet types excluding the conditionFields
     * @return Record with the fields
     */
    private static Object[] readParquetLogicalTypes(final Group datum, final List<String> conditionFields)
        throws IOException {

        final List<Type> fields = datum.getType().getFields();
        final Object[] vdpRecord = new Object[fields.size() - conditionFields.size()];
        int i = 0;
        for (final Type field : fields) {
            if (!conditionFields.contains(field.getName()))  {
                vdpRecord[i] = readTypes(datum, field);
            }
            i++;
        }
        return vdpRecord;
    }

    /**
     * Method for read parquet types. The method does the validation to know what type needs to be read.
     */
    private static Object readTypes(final Group datum, final Type valueList) throws IOException {
        
        if (valueList.isPrimitive()) {
            return readPrimitive(datum, valueList);
        }
        
        if (ParquetTypeUtils.isGroup(valueList)) {
            return readGroup(datum, valueList);
        }

        if (ParquetTypeUtils.isMap(valueList)) {
            return readMap(datum, valueList);
        }

        if (ParquetTypeUtils.isList(valueList)) {
            return readList(datum, valueList);
        }

        LOG.error("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");
        throw new IOException("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");

    }

    /**
     * Read method for parquet primitive types
     */
    private static Object readPrimitive(final Group datum, final Type field) throws IOException {

        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            final PrimitiveTypeName primitiveTypeName = field.asPrimitiveType().getPrimitiveTypeName();
            try {

                if (PrimitiveTypeName.BINARY.equals(primitiveTypeName)) {
                    if (stringType().equals(field.getLogicalTypeAnnotation())
                        || jsonType().equals(field.getLogicalTypeAnnotation())
                        || bsonType().equals(field.getLogicalTypeAnnotation())) {
                        return datum.getString(field.getName(), 0);

                    } else {
                        return datum.getBinary(field.getName(), 0).getBytes();
                    }
                    
                } else if (PrimitiveTypeName.BOOLEAN.equals(primitiveTypeName)) {
                    return  datum.getBoolean(field.getName(), 0);
                    
                } else if (PrimitiveTypeName.DOUBLE.equals(primitiveTypeName)) {
                    return datum.getDouble(field.getName(), 0);
                    
                } else if (PrimitiveTypeName.FLOAT.equals(primitiveTypeName)) {
                    return datum.getFloat(field.getName(), 0);
                    
                } else if (PrimitiveTypeName.INT32.equals(primitiveTypeName)) {
                    if (field.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation) {
                        final int scale = ((DecimalLogicalTypeAnnotation) field.asPrimitiveType().getLogicalTypeAnnotation()).getScale();
                        return new BigDecimal(BigInteger.valueOf(datum.getInteger(field.getName(), 0)), scale);

                    } else if (dateType().equals(field.getLogicalTypeAnnotation())) {
                        //   DATE fields really holds the number of days since 1970-01-01
                        final int days = datum.getInteger(field.getName(), 0);
                        // We need to add one day because the index start in 0.
                        final long daysMillis = TimeUnit.DAYS.toMillis(days + 1);
                        return new Date(daysMillis);

                    } else if (field.getLogicalTypeAnnotation() instanceof TimeLogicalTypeAnnotation) {
                        final long millisAfterMidnight = datum.getInteger(field.getName(), 0);
                        return new Time(millisAfterMidnight);

                    } else {
                        return datum.getInteger(field.getName(), 0);
                    }
                    
                } else if (PrimitiveTypeName.INT64.equals(primitiveTypeName)) {
                    if (field.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation) {
                        final int scale = ((DecimalLogicalTypeAnnotation) field.asPrimitiveType().getLogicalTypeAnnotation()).getScale();
                        return new BigDecimal(BigInteger.valueOf(datum.getLong(field.getName(), 0)), scale);

                    } else if (field.getLogicalTypeAnnotation() instanceof TimeLogicalTypeAnnotation) {
                        final long millisAfterMidnight = TimeUnit.MICROSECONDS.toMillis(datum.getLong(field.getName(), 0));
                        return new Time(millisAfterMidnight);

                    } else if (field.getLogicalTypeAnnotation() instanceof TimestampLogicalTypeAnnotation) {
                        final LogicalTypeAnnotation.TimeUnit unit =
                            ((TimestampLogicalTypeAnnotation) field.asPrimitiveType().getLogicalTypeAnnotation()).getUnit();
                        long milliseconds = datum.getLong(field.getName(), 0);
                        if (unit.equals(LogicalTypeAnnotation.TimeUnit.MICROS)) {
                            milliseconds = TimeUnit.MICROSECONDS.toMillis(milliseconds);
                        }

                        return new Timestamp(milliseconds);

                    } else {
                        return datum.getLong(field.getName(), 0);
                    }
                    
                } else if (PrimitiveTypeName.INT96.equals(primitiveTypeName)) {
                    final Binary binary = datum.getInt96(field.getName(), 0);
                    final long timestampMillis = ParquetTypeUtils.int96ToTimestampMillis(binary);
                    return new Date(timestampMillis);
                    
                } else if (primitiveTypeName.equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                    if (field.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation) {
                        final int scale = ((DecimalLogicalTypeAnnotation) field.asPrimitiveType().getLogicalTypeAnnotation()).getScale();
                        return new BigDecimal(new BigInteger(datum.getBinary(field.getName(), 0).getBytes()), scale);
                    }

                    return datum.getBinary(field.getName(), 0).getBytes();

                } else {
                    LOG.error("Type of the field " + field + ", not supported by the custom wrapper ");
                    throw new IOException("Type of the field " + field + ", not supported by the custom wrapper ");
                }
            } catch (final RuntimeException e) {
                LOG.warn("Error reading data", e);
            }
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field " + field + " is Required");
        }
        return null;
    }
    
    /**
     * Read method for parquet group types
     * @return Record with the fields
     */
    private static Object readGroup(final Group datum, final Type field) throws IOException {

        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            return readParquetLogicalTypes(datum.getGroup(field.getName(), 0));
        } else if (field.getRepetition() == Repetition.REQUIRED) {
            //Is required tipe in schema
            throw new IOException("The field " + field + " is Required");
        }
        LOG.trace("The group " + field.getName() + " doesn't exist");
        return null;
    }
    
    /**
     * Read method for parquet list types
     * @return Array with the fields
     */
    private static Object readList(final Group datum, final Type field) throws IOException {

        //This only works with the last parquet version 1.5.0 Previous versions maybe don't have a valid format.
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    final Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][1];
                    for (int j = 0; j < datumGroup.getFieldRepetitionCount(0); j++) {
                        final Group datumList = datumGroup.getGroup(0, j);
                        if (datumList != null && !datumList.getType().getFields().isEmpty()) {
                            vdpArrayRecord[j][0] = readTypes(datumList, datumList.getType().getFields().get(0));
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
    private static Object readMap(final Group datum, final Type field) throws IOException {

        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    final Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][2];
                    for (int j = 0; j < datumGroup.getFieldRepetitionCount(0); j++) {
                        final Group datumMap = datumGroup.getGroup(0, j);
                        if (datumMap != null && datumMap.getType().getFields().size() > 1) {
                            vdpArrayRecord[j][0] = readPrimitive(datumMap,datumMap.getType().getFields().get(0));
                            vdpArrayRecord[j][1] = readTypes(datumMap, datumMap.getType().getFields().get(0));
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

    /**
     * Method for read all the parquet types
     * @return Record with the fields
     */
    private static Object[] readParquetLogicalTypes(final Group datum) throws IOException {

        final List<Type> fields = datum.getType().getFields();
        final Object[] vdpRecord = new Object[fields.size()];
        int i = 0;
        for (final Type field : fields) {
            vdpRecord[i] = readTypes(datum, field);
            i++;
        }
        return vdpRecord;
    }
    
}
