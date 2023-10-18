## Spark Data Sync


This project aims to transfer data from several sources.

This project is made up of three config files which are necessary for the proper functioning of the program:


### **config/spark_config.yaml** 
This file contains the different configuration of the Spark Application. 

### **config/jdbc_connection.yaml** 
This file contains the information to connect to the database if required 

Exemple : 
```yaml
postgres :
  url : jdbc:postgresql://localhost:5432/postgres
  user : postgres
  password : postgres
```

### **config/jobs.yaml** 
This files contains the different jobs to transfer data from a source to a target. 

Exemple : 
```yaml
Job1:
  source:
    sourceType: csv
    filePath: path/test/test.csv
      column:
      filter:
      renamedColumn: [ [ count,id ] ]
      readTolerance : True
  target:
    targetType: parquet
    filePath: path/test/customers.parquet
    mode: overwrite
    partitionsNumber : 20
    partitionsColumn: [ORIGIN_COUNTRY_NAME]
  Job2:
    source:
      sourceType: parquet
      filePath: path/test/customers.parquet
      column:
      filter:
      renamedColumn: [ [ index,id ] ]
    target:
      targetType: postgres
      tableName: s1.test
      sourceKey: [ id ]
      targetKey: [ id ]
      mode: scd1
  Job3:
    source:
      sourceType: postgres
      tableName: s1.test
    target:
      targetType: parquet
      filePath: path/test/customers-sauv.parquet
      mode: overwrite
  Job4:
    source:
      sourceType : parquet
      filePath : test.csv
      key :
      column :
      filter :
    target :
      targetType : parquet
      filePath : testResult4.parquet
      key :
      mode : append
```