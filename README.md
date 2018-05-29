# Design 
This application has been designed as a **_local Spark application_**. 
As Spark needs at least 512MB of memory you need to have at least that amount of memory on your local execution environment.
The application will use as many as available cores on your local environment.



**512MB will be enough to handle large files even GBs files** 

_A Spark application can be tuned further but I think this is out of scope_

# Build the application 

From the project root :

```
export SPARK_LOCAL_IP='127.0.0.1' && mvn clean verify
```
As it is an application using Spark in local mode an **uber jar** with all the needed dependencies must be the one we will use to run the application.
The previous command will generate the uber jar **_mehdi-fffc.jar_** under **_target/_**.

Besides it will run unit and integration tests

# Run the application 

**_Running the application requires 3 args to be passed to the uber jar otherwise it will throw an exception_**
1. metafile path
2. fixed file path
3. CSV(s) output directory path 

You can set the amount of memory for the application using the VM -Xmx arg

You can run the application using this command pattern: 

```
java -Xmx<memory> -jar <path-to>/mehdi-fffc.jar <metafile-path> <fixed-file-path> <output-directory-path>
```

Example : 

```
java -Xmx512m -jar target/mehdi-fffc.jar "/tmp/fffc/metadata.csv" "/tmp/fffc/fixed-file.txt" "/tmp/fffc/output"
```

Running the application successfully will generate csv files under <output-directory> :

- _SUCCESS is a flag for success
- part-0000* for the resulting CVS files

**the number of CSV files will be : MAX**_(the size of the fixed input file / 128MB, cores number available in your env)_ 

# Errors reporting and logging
Errors will be reported in a file under your **_<CURRENT_DIR>/log_**
The file will have this naming pattern 
```
fffc-errors-yyyyMMdd'T'HHmm.log
```
For more verbose logging, you can check the log file under the same directory having this naming pattern 
```
fffc-verbose-yyyyMMdd'T'HHmm.log
```

**_Empty log files still be generated, even when the application terminates without any error._**

# Building the application and avoid running integration tests

Use this command

```
export SPARK_LOCAL_IP='127.0.0.1' &&  mvn clean package
```




