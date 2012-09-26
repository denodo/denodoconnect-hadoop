package com.denodo.devkit.hdfs.wrapper.exceptions;

public class UnsupportedTypeException extends RuntimeException {
    private static final long serialVersionUID = -3642408615843489702L;

    public UnsupportedTypeException(String type) {
        super("Type " + type + " is not supported");
    }

    public UnsupportedTypeException() {
        super();
    }

}
