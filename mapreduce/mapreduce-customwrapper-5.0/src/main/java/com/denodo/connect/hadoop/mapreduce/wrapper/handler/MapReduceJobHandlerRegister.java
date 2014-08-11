/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2014, denodo technologies (http://www.denodo.com)
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
package com.denodo.connect.hadoop.mapreduce.wrapper.handler;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;


public final class MapReduceJobHandlerRegister {

    private static Map<String, Class<? extends MapReduceJobHandler>> handlers = scanHandlers();
    
    private MapReduceJobHandlerRegister() {
        
    }
    
    private static Map<String, Class<? extends MapReduceJobHandler>> scanHandlers() {
        
        Map<String, Class<? extends MapReduceJobHandler>> handlers = new HashMap<String, Class<? extends MapReduceJobHandler>>();

        Reflections reflections = new Reflections("com.denodo.connect.hadoop.mapreduce.wrapper.handler");
        Set<Class<? extends MapReduceJobHandler>> subTypes = reflections.getSubTypesOf(MapReduceJobHandler.class);
        for (Class<? extends MapReduceJobHandler> subType : subTypes) {
            if (!Modifier.isAbstract(subType.getModifiers())) {
             handlers.put(getDisplayName(subType), subType);
            }
        }
        
        return handlers;
    }
   
    private static String getDisplayName(Class<? extends MapReduceJobHandler> subType) {

        String displayName = subType.getSimpleName();
        int index = displayName.indexOf(MapReduceJobHandler.class.getSimpleName());
        if (index != -1) {
            displayName = displayName.substring(0, index);
        }
        return displayName;
    }

    public static Collection<String> getHandlerNames() {
        return handlers.keySet();
    }
    
    public static Class<? extends MapReduceJobHandler> getHandler(String displayName) {
        return handlers.get(displayName);
    }
    

}
