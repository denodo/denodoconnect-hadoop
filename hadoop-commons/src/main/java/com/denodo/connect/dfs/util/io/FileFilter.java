/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2018, Denodo Technologies (http://www.denodo.com)
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
package com.denodo.connect.dfs.util.io;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.denodo.vdb.engine.customwrapper.condition.CustomWrapperCondition;

public class FileFilter implements PathFilter {

    private static final Logger LOG = LoggerFactory.getLogger(FileFilter.class);

    /* A _SUCCESS empty file is created on the successful completion of a MapReduce job; we will ignore this kind of files. */
    private static final String SUCCESS_FILE_NAME = "_SUCCESS";
    
    private String pattern;
    private List<String> partitionFields;
    private Collection<String> conditionFields;
    private CustomWrapperCondition condition;
    
    public FileFilter(final String pattern) {
        this.pattern = pattern;
    }

    public void addPartitionConditions(final List<String> partitionFields, final Collection<String> conditionFields,
        final CustomWrapperCondition condition) {

        this.partitionFields = partitionFields;
        this.conditionFields = conditionFields;
        this.condition = condition;
    }

    @Override
    public boolean accept(final Path file) {

        boolean accept = !SUCCESS_FILE_NAME.equals(file.getName()) && matchFilePattern(file.toUri().getPath());
        if (this.partitionFields != null) {
            accept = accept && matchPartition(file);
        }

        return accept;
    }

    private boolean matchFilePattern(final String filePath) {

        if (StringUtils.isNotBlank(this.pattern)) {
            return filePath.matches(this.pattern);
        }

        return true;
    }

    private boolean matchPartition(final Path path) {

        boolean match = true;

        final List<Type> partitionFields = PartitionUtils.getPartitionFields(path);
        if (!partitionFields.isEmpty()) {

            final List<Comparable> partitionValues = PartitionUtils.getPartitionValues(path);
            int i = 0;
            final Map<String, Comparable> partitionFieldValues = new HashMap<>();
            for (final Type partitionField : partitionFields) {
                partitionFieldValues.put(partitionField.getName(), partitionValues.get(i++));
            }

            if (!Collections.disjoint(this.conditionFields, partitionFieldValues.keySet())) {
                match = ConditionUtils.evalPartitionCondition(this.condition, partitionFieldValues);

                if (!match && LOG.isDebugEnabled()) {
                    LOG.debug(path + " has been pruned");
                }
            }
        }

        return match;
    }

}
