HSecordaryIndex
===============

## IndexGenerator
    Index generating coprocessor, generate index logs.
    
## IndexWriter
    Consume index logs, batch put into index table. 
    java -jar IndexWriter-1.0-jar-with-dependencies.jar
    
    schema:
    property_sof-dsk_index 1_ref_20130101 value:uid
      
## IndexPorter
    Perform daily index copy task.

## IndexImport
    Transfer user info from mysql to hbase.
    
    command:
    java -jar IndexImport-1.0-jar-with-dependencies.jar /data/loadmysqltohdfs sof-dsk xlfc
    java -jar IndexImport-1.0-jar-with-dependencies.jar remove sof-dsk xlfc
    
    schema:
    property_sof-dsk_1 uid value:value
    property_sof-dsk_2