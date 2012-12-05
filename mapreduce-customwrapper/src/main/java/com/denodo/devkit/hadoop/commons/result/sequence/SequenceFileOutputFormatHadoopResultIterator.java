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
package com.denodo.devkit.hadoop.commons.result.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.denodo.devkit.hadoop.commons.exception.InternalErrorException;
import com.denodo.devkit.hadoop.commons.result.IHadoopResultIterator;
import com.denodo.devkit.hadoop.util.HadoopUtils;
import com.denodo.devkit.hadoop.util.configuration.HadoopConfigurationUtils;
import com.denodo.devkit.hadoop.util.type.TypeUtils;

/**
 * Class to iterate over a {@link SequenceFile}
 *
 */
public class SequenceFileOutputFormatHadoopResultIterator 
implements IHadoopResultIterator {

    private static final Logger logger = Logger
            .getLogger(SequenceFileOutputFormatHadoopResultIterator.class);

    private Configuration configuration;
    private String dataNodeIp;
    private String dataNodePort;
    private String hadoopKeyClass;
    private String hadoopValueClass;
    private Path outputPath;
    private boolean deleteOutputPathAfterReadOutput;


    private FileSystem fileSystem;
    private FileStatus[] fss;
    private int currentFileStatusIndex = -1;
    private SequenceFile.Reader currentReader;


    public SequenceFileOutputFormatHadoopResultIterator(String dataNodeIp, String dataNodePort,
            String hadoopKeyClass, String hadoopValueClass, Path outputPath, boolean deleteOutputPathAfterReadOutput) {
        super();

        this.dataNodeIp = dataNodeIp;
        this.dataNodePort = dataNodePort;
        this.hadoopKeyClass = hadoopKeyClass;
        this.hadoopValueClass = hadoopValueClass;
        this.outputPath = outputPath;
        this.deleteOutputPathAfterReadOutput = deleteOutputPathAfterReadOutput;

        try {
            this.configuration = HadoopConfigurationUtils.getConfiguration(
                    dataNodeIp, dataNodePort, 
                    hadoopKeyClass, hadoopValueClass);
            this.fileSystem = FileSystem.get(this.configuration);

            if (logger.isDebugEnabled()) {
                logger.debug("FileSystem is: " + this.fileSystem.getUri()); //$NON-NLS-1$
                logger.debug("Output path is: " + outputPath); //$NON-NLS-1$
            }

            this.fss = this.fileSystem.listStatus(outputPath);
        } catch (IOException e) {
            throw new InternalErrorException("There has been an error reading files from output folder " + this.outputPath, e); //$NON-NLS-1$
        }
    }

    /* (non-Javadoc)
     * @see com.denodo.devkit.hadoop.commons.result.IHadoopResultIterator#readNext(org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable)
     */
    @Override
    public <K extends Writable, V extends Writable> boolean readNext(K key, V value) {

        try {
            if (this.fss == null || this.fss.length == 0) {
                return false;
            }

            // If first time
            if (this.currentReader == null) {
                this.currentFileStatusIndex++;
                this.currentReader = new SequenceFile.Reader(this.configuration, SequenceFile.Reader.file(this.fss[this.currentFileStatusIndex].getPath()));
            }

            if (this.currentReader.next(key, value)) {
                // Has next -> Values are in key and value 
                return true;
            } 

            // This reader does not have anything to be read --> should take next one
            // Close currentReader
            this.currentReader.close();
            
            // Take next file status reader
            this.currentFileStatusIndex++;
            if (this.fss.length > this.currentFileStatusIndex) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Reading path: " + this.fss[this.currentFileStatusIndex].getPath().getName()); //$NON-NLS-1$
                }
                this.currentReader = new SequenceFile.Reader(this.configuration, SequenceFile.Reader.file(this.fss[this.currentFileStatusIndex].getPath()));
                if (this.currentReader.next(key, value)) {
                    // Has next -> Values are in key and value -> do anything else   
                    return true;
                }
                return readNext(key, value);
            }
            if (this.deleteOutputPathAfterReadOutput) {
                HadoopUtils.deleteFile(this.dataNodeIp, this.dataNodePort, 
                        this.hadoopKeyClass, this.hadoopValueClass, this.outputPath);
            }
            
            this.fileSystem.close();
            return false;
        } catch (IOException e) {
            throw new InternalErrorException("There has been an error reading results from output folder " + this.outputPath, e); //$NON-NLS-1$
        } 
    }

    /* (non-Javadoc)
     * @see com.denodo.devkit.hadoop.commons.result.IHadoopResultIterator#getInitKey()
     */
    @SuppressWarnings("unchecked")
    @Override
    public <K extends Writable> K getInitKey() {
        return (K) TypeUtils.getInitKey(this.hadoopKeyClass, this.configuration);
    }
 
    /* (non-Javadoc)
     * @see com.denodo.devkit.hadoop.commons.result.IHadoopResultIterator#getInitValue()
     */
    @SuppressWarnings("unchecked")
    @Override
    public <V extends Writable> V getInitValue() {
        return (V) TypeUtils.getInitValue(this.hadoopValueClass, this.configuration);
    }

}
