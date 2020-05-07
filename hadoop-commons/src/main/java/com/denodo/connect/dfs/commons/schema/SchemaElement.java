package com.denodo.connect.dfs.commons.schema;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.parquet.schema.PrimitiveType;



public class SchemaElement {

    private String name;
    private Class<?> type;
    private PrimitiveType.PrimitiveTypeName sourceType;
    private Collection<SchemaElement> elements;
    private boolean isNullable = false;

    public SchemaElement(final String name, final Class<?> type) {

        this.name = name;
        this.type = type;
        this.elements = new ArrayList<>();
    }
    
    public SchemaElement(final String name, final Class<?> type, final boolean isNullable) {

        this.name = name;
        this.type = type;
        this.elements = new ArrayList<>();
        this.isNullable = isNullable;
    }

    public SchemaElement(
        final String name, final Class<?> type, final PrimitiveType.PrimitiveTypeName sourceType, final boolean isNullable) {

        this.name = name;
        this.type = type;
        this.sourceType = sourceType;
        this.elements = new ArrayList<>();
        this.isNullable = isNullable;
    }

    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setType(final Class<?> type) {
        this.type = type;
    }

    public PrimitiveType.PrimitiveTypeName getSourceType() { return this.sourceType; }

    public void setSourceType(final PrimitiveType.PrimitiveTypeName sourceType) { this.sourceType = sourceType; }

    public Collection<SchemaElement> getElements() {
        return this.elements;
    }
    
    public void add(final SchemaElement element) {
        this.elements.add(element);
    }

    public boolean isNullable() {
        return this.isNullable;
    }

    public void setNullable(final boolean isNullable) {
        this.isNullable = isNullable;
    }

}
