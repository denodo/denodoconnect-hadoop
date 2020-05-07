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
package com.denodo.connect.dfs.reader;

import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

import com.denodo.connect.dfs.commons.naming.Parameter;
import com.denodo.connect.dfs.util.io.PartitionUtils;
import com.denodo.connect.dfs.util.schema.VDPSchemaUtils;
import com.denodo.connect.dfs.util.type.ParquetTypeUtils;
import com.denodo.vdb.engine.customwrapper.expression.CustomWrapperFieldExpression;


public class DFSParquetFileReader implements DFSFileReader {

    private static final  Logger LOG = LoggerFactory.getLogger(DFSParquetFileReader.class);

    private ParquetReader<Group> dataFileReader;
    private List<CustomWrapperFieldExpression> projectedFields;
    private String pathValue;

    private Long startingPos;
    private Long endingPos;

    private Map<String, Object> additionalValuesByName;

    public DFSParquetFileReader(final Configuration conf, final Path path,  final boolean includePathValue,
        final FilterCompat.Filter filter, final MessageType querySchema, final List<CustomWrapperFieldExpression> projectedFields)
        throws IOException {

        this(conf, path, includePathValue, filter, querySchema, projectedFields, null, null);
    }

    public DFSParquetFileReader(final Configuration conf, final Path path,  final boolean includePathValue,
        final FilterCompat.Filter filter, final MessageType querySchema, final List<CustomWrapperFieldExpression> projectedFields,
        final Long startingPos, final Long endingPos) throws IOException {

        this.pathValue = path.toString();
        this.projectedFields = projectedFields;
        this.startingPos = startingPos;
        this.endingPos = endingPos;

        this.additionalValuesByName = getAdditionalValuesByName(path, includePathValue, projectedFields, querySchema);

        openReader(path, conf, filter, querySchema);

    }

    private static Map<String, Object> getAdditionalValuesByName(final Path path, final boolean includePathValue,
        final List<CustomWrapperFieldExpression> projectedFields, final MessageType querySchema) {

        final Map<String, Object> valuesByName = getProjectedPartitionValuesByName(path, projectedFields, querySchema);

        if (includePathValue) {
            valuesByName.put(Parameter.FULL_PATH, path.toString());

        }

        return valuesByName;
    }

    private static Map<String, Object> getProjectedPartitionValuesByName(final Path path, final List<CustomWrapperFieldExpression> projectedFields,
        final MessageType querySchema) {

        final Map<String, Object> projectedPartitionValuesByName = new HashMap<>();

        final List<Type> partitionFields = PartitionUtils.getPartitionFields(path);
        final List<Comparable> partitionValues = PartitionUtils.getPartitionValues(path);
        int i = 0;
        for (final Type partitionField : partitionFields) {
            if (!querySchema.containsField(partitionField.getName())
                && VDPSchemaUtils.isProjected(partitionField.getName(), projectedFields)) {

                projectedPartitionValuesByName.put(partitionField.getName(), partitionValues.get(i));
            }
            i++;
        }


        return projectedPartitionValuesByName;

    }

    private void openReader(final Path path, final Configuration configuration, final FilterCompat.Filter filter,
        final MessageType querySchema) throws IOException {

        try {
            configuration.set(ReadSupport.PARQUET_READ_SCHEMA, querySchema.toString());

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

            final Object data = doRead();
            if (data != null) {
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
            return buildRecord(data, this.projectedFields, this.additionalValuesByName);
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

    /*
     * Reads the file parquet and adding extra columns not included in the Parquet file like fullpath and others.
     */
    private static Object[] buildRecord(final Group datum, final List<CustomWrapperFieldExpression> projectedFields,
        final Map<String, Object> additionalValuesByName) throws IOException {

        final Object[] vdpRecord = new Object[projectedFields.size()];

        final List<Type> fields = datum.getType().getFields();
        final Map<String, Type> fielsdByName = getFieldsByName(fields);

        int i = 0;
        for (final CustomWrapperFieldExpression projectedField : projectedFields) {
            if (fielsdByName.containsKey(projectedField.getName())) {
                vdpRecord[i++] = readParquetTypes(datum, fielsdByName.get(projectedField.getName()));
            } else if (additionalValuesByName.containsKey(projectedField.getName())) {
                vdpRecord[i++] = additionalValuesByName.get(projectedField.getName());
            }
        }

        return vdpRecord;
    }

    private static Map<String, Type> getFieldsByName(final List<Type> fields) {

        final Map<String, Type> fielsdByName = new HashMap<>(fields.size());
        for (final Type field : fields) {
            fielsdByName.put(field.getName(), field);
        }

        return fielsdByName;
    }

    /**
     * Method for read parquet types. The method does the validation to know what type needs to be read.
     */
    private static Object readParquetTypes(final Group datum, final Type valueList) throws IOException {
        
        if (valueList.isPrimitive()) {
            return readParquetPrimitive(datum, valueList);
        }
        
        if (ParquetTypeUtils.isGroup(valueList)) {
            return readParquetGroup(datum, valueList);
        }

        if (ParquetTypeUtils.isMap(valueList)) {
            return readParquetMap(datum, valueList);
        }

        if (ParquetTypeUtils.isList(valueList)) {
            return readParquetList(datum, valueList);
        }

        LOG.error("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");
        throw new IOException("Type of the field " + valueList.toString() + ", does not supported by the custom wrapper ");

    }

    /**
     * Read method for parquet primitive types
     */
    private static Object readParquetPrimitive(final Group datum, final Type field) throws IOException {

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
    private static Object readParquetGroup(final Group datum, final Type field) throws IOException {

        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            return buildRecord(datum.getGroup(field.getName(), 0));
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
    private static Object readParquetList(final Group datum, final Type field) throws IOException {

        //This only works with the last parquet version 1.5.0 Previous versions maybe don't have a valid format.
        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    final Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][1];
                    for (int j = 0; j < datumGroup.getFieldRepetitionCount(0); j++) {
                        final Group datumList = datumGroup.getGroup(0, j);
                        if (datumList != null && !datumList.getType().getFields().isEmpty()) {
                            vdpArrayRecord[j][0] = readParquetTypes(datumList, datumList.getType().getFields().get(0));
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
    private static Object readParquetMap(final Group datum, final Type field) throws IOException {

        if (datum.getFieldRepetitionCount(field.getName()) > 0) {
            try {
                final Group datumGroup = datum.getGroup(field.getName(), 0);
                if (datumGroup.getFieldRepetitionCount(0) > 0) {
                    final Object[][] vdpArrayRecord = new Object[datumGroup.getFieldRepetitionCount(0)][2];
                    for (int j = 0; j < datumGroup.getFieldRepetitionCount(0); j++) {
                        final Group datumMap = datumGroup.getGroup(0, j);
                        if (datumMap != null && datumMap.getType().getFields().size() > 1) {
                            vdpArrayRecord[j][0] = readParquetPrimitive(datumMap,datumMap.getType().getFields().get(0));
                            vdpArrayRecord[j][1] = readParquetTypes(datumMap, datumMap.getType().getFields().get(0));
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
    private static Object[] buildRecord(final Group datum) throws IOException {

        final List<Type> fields = datum.getType().getFields();
        final Object[] vdpRecord = new Object[fields.size()];
        int i = 0;
        for (final Type field : fields) {
            vdpRecord[i] = readParquetTypes(datum, field);
            i++;
        }
        return vdpRecord;
    }

}
