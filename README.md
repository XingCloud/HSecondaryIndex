HSecordaryIndex
===============

## IndexGenerator
Index generating coprocessor, generate index logs.
    
## IndexWriter
Consume index logs, batch put into index table. 
    
    java -jar IndexWriter-1.0-jar-with-dependencies.jar
      
## IndexPorter
Perform daily index copy task.

## IndexImport
Transfer user info from mysql to hbase.
    
command:
    
    java -jar IndexImport-1.0-jar-with-dependencies.jar /data/loadmysqltohdfs sof-dsk xlfc
    java -jar IndexImport-1.0-jar-with-dependencies.jar remove sof-dsk xlfc

## Schema
Please refer to the wiki
