package com.denodo.connect.hadoop.hdfs.commons.schema;

import java.util.ArrayList;
import java.util.Collection;


public class SchemaElement {

    private String name;
    private Class<?> type;
    private Collection<SchemaElement> elements;

    public SchemaElement(String name, Class<?> type) {

        this.name = name;
        this.type = type;
        this.elements = new ArrayList<SchemaElement>();
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

    public Collection<SchemaElement> getElements() {
        return this.elements;
    }

    public void add(SchemaElement element) {
        this.elements.add(element);
    }


}
