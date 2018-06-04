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

import com.denodo.connect.hadoop.hdfs.commons.schema.SchemaElement;
import com.denodo.connect.hadoop.hdfs.util.schema.ParquetSchemaUtils;
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
        final SchemaElement schemaElement = ParquetSchemaUtils.buildSchema(group, group.getType().getName());
        closeReader();

        return schemaElement;
    }

    @Override
    public Object doRead() throws IOException {

        final Group data = this.dataFileReader.read();
        if (data!=null) {
            return read( data, this.projectedFields);
        }

        return null;
    }

    @Override
    public void closeReader() throws IOException {
        if (this.dataFileReader != null) {
            this.dataFileReader.close();
        }
    }

    private static Object read( final Group datum, final List<CustomWrapperFieldExpression> projectedFields) throws IOException {



        final List<Type> fields = datum.getType().getFields();
        final Object[] vdpRecord = new Object[fields.size()];

        int i= 0;
        for (final CustomWrapperFieldExpression fieldExpression : projectedFields) {

            for(final Type field:  fields){
                if(field.getName().equals(fieldExpression.getName())){
                    if(field.isPrimitive()){
                        final PrimitiveTypeName   primitiveTypeName = field.asPrimitiveType().getPrimitiveTypeName();
                        //We only manage primitiveTypes(BINARY,BOOLEAN; DOUBLE, FLOAT, INT32, INT64, INT96, FIXED_LEN_BYTE_ARRAY)
                        //If it were necessary to manage other type to differentiate among the originalTypes(DATE, MAP....), an ad hoc development would be required 

                        try{
                            if(primitiveTypeName.equals(PrimitiveTypeName.BINARY)) {
                                if(field.getOriginalType()!=null){
                                    if(field.getOriginalType().equals(OriginalType.UTF8)){
                                        vdpRecord[i] = datum.getString(i, 0);
                                    } else if(field.getOriginalType().equals(OriginalType.JSON)){
                                        vdpRecord[i] = datum.getString(i, 0);
                                    } else if(field.getOriginalType().equals(OriginalType.BSON)){
                                        vdpRecord[i] = datum.getString(i, 0);
                                    } else{
                                        vdpRecord[i] = datum.getBinary(i, 0).getBytes(); 
                                    }
                                }else{
                                    vdpRecord[i] = datum.getBinary(i, 0).getBytes();   
                                }

                            }else if(primitiveTypeName.equals(PrimitiveTypeName.BOOLEAN)) {
                                vdpRecord[i] =  datum.getBoolean(i, 0);

                            }else if(primitiveTypeName.equals(PrimitiveTypeName.DOUBLE)) {
                                vdpRecord[i] = datum.getDouble(i, 0);

                            }else if(primitiveTypeName.equals(PrimitiveTypeName.FLOAT)) {
                                vdpRecord[i] = datum.getFloat(i, 0);

                            }else if(primitiveTypeName.equals(PrimitiveTypeName.INT32)) {
                                if (OriginalType.DECIMAL.equals(field.getOriginalType())) {
                                    final int scale = field.asPrimitiveType().getDecimalMetadata().getScale();
                                    vdpRecord[i] = new BigDecimal(BigInteger.valueOf(datum.getInteger(i, 0)), scale);
                                } else if (OriginalType.DATE.equals(field.getOriginalType())) {
                                    //   DATE fields really holds the number of days since 1970-01-01
                                    final int days = datum.getInteger(i,0);
                                    final long daysMillis = TimeUnit.DAYS.toMillis(days); 
                                    vdpRecord[i] = new Date(daysMillis);
                                } else {
                                    vdpRecord[i] = datum.getInteger(i, 0);
                                }

                            }else if(primitiveTypeName.equals(PrimitiveTypeName.INT64)) {
                                //we dont differentiate INT64 from TIMESTAMP_MILLIS original types
                                vdpRecord[i] = datum.getLong(i, 0);

                            }else if(primitiveTypeName.equals(PrimitiveTypeName.INT96)) {
                                vdpRecord[i] = datum.getInt96(i, 0);

                            }else if(primitiveTypeName.equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {

                                vdpRecord[i] =datum.getBinary(i, 0).getBytes();
                            }else{
                                logger.error("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                                throw new IOException("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                            }
                        }catch(final RuntimeException e){
                            logger.warn("It was a error reading data", e);
                        }
                        i++;
                    }else{
                        logger.error("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                        throw new IOException("Type of the field "+ field.toString()+", does not supported by the custom wrapper ");
                    }
                    break;
                }
            }
        }
        return vdpRecord;
    }



}
