HDFS Custom Wrappers

This project contains 3 custom wrappers for reading files stored in HDFS:

* HdfsDelimitedTextFileWrapper
HDFS File Connector for reading key-value delimited text files stored in HDFS (Hadoop Distributed File System).
You will be asked Namenode host, Namenode port, file path and file separator.
If everything works fine, the key-value pairs contained in the file will be returned by the wrapper


* HdfsSequenceFileWrapper
HDFS File Connector for reading Sequence files stored in HDFS (Hadoop Distributed File System)
You will be asked Namenode host, Namenode port, file path, Hadoop key class and Hadoop value class. 
If everything works fine, the key-value pairs contained in the file will be returned by the wrapper

* HdfsMapFileWrapper
HDFS File Connector for reading Map files stored in HDFS (Hadoop Distributed File System)
You will be asked Namenode host, Namenode port, file path, Hadoop key class and Hadoop value class. 
If everything works fine, the key-value pairs contained in the file will be returned by the wrapper


#############################
##                         ##
## GUIDELINE TO INSTALL IT ##
##                         ##
#############################

- To use the Custom Wrapper you need to:

1) mvn package

2) Import the jar file of the custom wrapper denodo-hdfs-customwrapper-1.0-SNAPSHOT-jar-with-dependencies.jar
(Admin Tool > File > Extensions > Jar Management > Create)

More information of usage can be found in: https://docs.google.com/a/denodo.com/document/d/1bM3BESmBFtGWZV4v9XaJ0Xz2GunyVQul66ufl9rk2Tg/edit#heading=h.606pxv3u3fy6