/*
 * =============================================================================
 * 
 *   This software is part of the denodo developer toolkit.
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
package com.denodo.devkit.hadoop.commons.exception;

import org.apache.hadoop.fs.Path;


public class DeleteFileException extends InternalErrorException {

    private static final long serialVersionUID = -395472082514655225L;

    public DeleteFileException(Path file) {
        super("There has been an error deleting file: " + file); //$NON-NLS-1$
    }
}
