package com.denodo.devkit.hadoop.commons.naming;

public class ParameterNaming {

	//Parameters
    public static String HOST_IP = "Host IP";
    public static String HOST_PORT = "Host port";
    public static String HOST_USER = "Host user";
    public static String HOST_PASSWORD = "Host password";
    public static String HOST_TIMEOUT = "Host timeout";
    
    public static String PATH_TO_JAR_IN_HOST = "Path to jar in host";
    public static String MAIN_CLASS_IN_JAR = "Main class in jar";
	
    // String with the parameters (i.e. "This is a test" 4 6 "username")
	public static String MAPREDUCE_PARAMETERS = "MapReduce parameters (it will be added to the call to the MapReduce job)";
	
	public static String HADOOP_KEY = "KEY";
	public static String HADOOP_KEY_CLASS = "KEY_CLASS"; // Hadoop class
    public static String HADOOP_VALUE = "VALUE";
    public static String HADOOP_VALUE_CLASS = "VALUE_CLASS"; // Hadoop class
    
    public static String CLASS_IMPLEMENTING_IHADOOPTASKHANDLER = "CLASS_IMPLEMENTING_IHADOOPTASKHANDLER";
}
