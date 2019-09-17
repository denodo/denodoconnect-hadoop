package com.denodo.connect.hadoop.hdfs.commons.schema;

import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.Collection;



public class SchemaElement {

    private String name;
    private Class<?> type;
    private PrimitiveType.PrimitiveTypeName sourceType;
    private Collection<SchemaElement> elements;
    private boolean isNullable = false;

    public SchemaElement(String name, Class<?> type) {

        this.name = name;
        this.type = type;
        this.elements = new ArrayList<SchemaElement>();
    }
    
    public SchemaElement(String name, Class<?> type, boolean isNullable) {

        this.name = name;
        this.type = type;
        this.elements = new ArrayList<SchemaElement>();
        this.isNullable = isNullable;
    }

    public SchemaElement(String name, Class<?> type, PrimitiveType.PrimitiveTypeName sourceType, boolean isNullable) {

        this.name = name;
        this.type = type;
        this.sourceType = sourceType;
        this.elements = new ArrayList<SchemaElement>();
        this.isNullable = isNullable;
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

    public PrimitiveType.PrimitiveTypeName getSourceType() { return this.sourceType; }

    public void setSourceType(PrimitiveType.PrimitiveTypeName sourceType) { this.sourceType = sourceType; }

    public Collection<SchemaElement> getElements() {
        return this.elements;
    }
    
    public void add(SchemaElement element) {
        this.elements.add(element);
    }

    public boolean isNullable() {
        return this.isNullable;
    }

    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

}
