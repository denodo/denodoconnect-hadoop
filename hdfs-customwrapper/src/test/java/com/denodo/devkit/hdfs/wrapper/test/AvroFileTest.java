/*
 * =============================================================================
 * 
 *   This software is part of the DenodoConnect component collection.
 *   
 *   Copyright (c) 2012, denodo technologies (http://www.denodo.com)
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
package com.denodo.devkit.hdfs.wrapper.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroFileTest {

    public static void main(String[] args) throws IOException {
        String host = "192.168.73.132";
        int port = 8020;
        String avsc_file = "/avro/WeatherComplex2.avsc";

        // String avsc_file = "/avro/WeatherComplex.avsc";
        // String avro_file = "/avro/WeatherComplex.avro";

        Schema avro_schema = null;
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + host + ":" + port);
        Path avsc_path = new Path(avsc_file);
        FileSystem fileSystem;
        FSDataInputStream dataInputStream = null;
        try {
            fileSystem = FileSystem.get(conf);
            dataInputStream = fileSystem.open(avsc_path);
            avro_schema = Schema.parse(dataInputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // // DATUM 1
        GenericRecord datum1 = new GenericData.Record(avro_schema);
        datum1.put("station", new Utf8("Aemet"));
        datum1.put("temp", new Integer("35"));
        datum1.put("time", new Long("201210111207"));
        List addresses = new ArrayList();
        GenericRecord addr1 = new GenericData.Record(avro_schema.getFields()
                .get(3).schema().getElementType());
        addr1.put("street", "Gran Via 32");
        addr1.put("city", "Madrid");
        addr1.put("state", "Madrid");
        GenericRecord addr2 = new GenericData.Record(avro_schema.getFields()
                .get(3).schema().getElementType());
        addr2.put("street", "Praza de Galicia");
        addr2.put("city", "Santiago de Compostela");
        addr2.put("state", "Galicia");
        addresses.add(addr1);
        addresses.add(addr2);
        datum1.put("addresses", addresses);
        File file = new File("WeatherComplex2.avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
                avro_schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
                writer);
        dataFileWriter.create(avro_schema, file);
        dataFileWriter.append(datum1);
        dataFileWriter.close();
        // // // DATUM 1
        // GenericRecord datum1 = new GenericData.Record(avro_schema);
        // datum1.put("station", new Utf8("Aemet"));
        // datum1.put("temp", new Integer("35"));
        // datum1.put("time", new Long("201210111207"));
        // List<String> tags = new ArrayList<String>();
        // tags.add("sunny");
        // tags.add("hot");
        // datum1.put("tags", tags);
        // GenericRecord addr1 = new GenericData.Record(avro_schema.getFields()
        // .get(4).schema());
        // addr1.put("street", "Gran Via 32");
        // addr1.put("city", "Madrid");
        // addr1.put("state", "Madrid");
        // datum1.put("address", addr1);
        //
        // // DATUM 2
        // GenericRecord datum2 = new GenericData.Record(avro_schema);
        // datum2.put("station", new Utf8("Meteogalicia"));
        // datum2.put("temp", new Integer("25"));
        // datum2.put("time", new Long("201210111207"));
        // List<String> tags2 = new ArrayList<String>();
        // tags2.add("foggy");
        // tags2.add("cloudy");
        // datum2.put("tags", tags2);
        // GenericRecord addr2 = new GenericData.Record(avro_schema.getFields()
        // .get(4).schema());
        // addr2.put("street", "Praza de Galicia");
        // addr2.put("city", "Santiago de Compostela");
        // addr2.put("state", "Galicia");
        // datum2.put("address", addr2);
        //
        // File file = new File("WeatherComplex.avro");
        // DatumWriter<GenericRecord> writer = new
        // GenericDatumWriter<GenericRecord>(
        // avro_schema);
        // DataFileWriter<GenericRecord> dataFileWriter = new
        // DataFileWriter<GenericRecord>(
        // writer);
        // dataFileWriter.create(avro_schema, file);
        // dataFileWriter.append(datum1);
        // dataFileWriter.append(datum2);
        // dataFileWriter.close();

        // File to read
        Path path = new Path("WeatherComplex2.avro");
        FsInput inputFile = null;
        DataFileReader<GenericData.Record> dataFileReader = null;
        try {
            inputFile = new FsInput(path, conf);
            DatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(
                    avro_schema);
            dataFileReader = new DataFileReader<GenericData.Record>(inputFile,
                    reader);
            for (GenericData.Record avro_record : dataFileReader) {
                System.out.println(avro_record);
            }

        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }
}
