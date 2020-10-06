# Arrow
Sample Java programs and Python scripts for writing/reading to Arrow
--------------------------------------------------------------------

1. ArrowPlasmaTestJava: simple Java program which writes Arrow record batches to a Plasma in-memory object store (which must be running at "/tmp/plasma")

2. ArrowTestJava: simple Java program which writes Arrow record batches to an Arrow file

3. CT2Arrow: Java program which reads data from a CT source and writes it as a record batch either to an Arrow output file ("*.arrow") or to a Plasma in-memory object store; this was the culmination of JPW's Java/Arrow development in the Phase I project.

 - to build CT2Arrow:  ./gradlew build

 - the JAR file is located at CT2Arrow/build/libs

 - usage information is available by executing:  java  -jar CT2Arrow.jar  -help

 - sample execute command; in this case, we read from CT source "PHM08", we trigger off CT channel "unit.i32", we write to Plasma (this is the "-p" option), we display debug information (this is the "-x" option), and we read in a total of 7 CT channels:

java -jar CT2Arrow.jar -s PHM08 -t unit.i32 -p -x -chans "unit.i32,time.i32,op1.f32,op2.f32,op3.f32,sensor01.f32,sensor02.f32"

4. OBD2Arrow: Java program which reads OBD data from an input file and write it out as Arrow record batches to an Arrow file
  - sample input file: Data/OBD/v2/dailyRoutes.csv (I think this is a somewhat cleaned-up version of "exp1_14drivers_14cars_dailyRoutes.csv" from https://www.kaggle.com/cephasax/obdii-ds3?select=exp1_14drivers_14cars_dailyRoutes.csv)
  - output file: Data/OBD/v2/dailyRoutes.arrow (there are 100 data rows (records) per batch except for the last batch (which contains 28 rows); total of 951 batches)

5. PHM08_to_Plasma: Java program which reads data from a PHM08 input file out to Apache Plasma in-memory object store
  - the PHM08 data is from the NASA jet engine prognostics challenge; from our shared repository, see Data/PHM08 or search for "PHM08 Challenge Data Set" at https://ti.arc.nasa.gov/tech/dash/groups/pcoe/prognostic-data-repository/#turbofan or https://ti.arc.nasa.gov/tech/dash/groups/pcoe/prognostic-data-repository/publications/#phm08_challenge

6. SamplePythonScripts:

  - read_arrow_test_file.py: Python script which reads Arrow data from a file; can use the "test.arrow" file contained in this same folder as an input file (this is the Arrow file written out by our sample "ArrowTestJava" application)
  
  - read_from_arrow_plasma.py: Python script which reads objects from Plasma memory store (located at "/tmp/plasma"); this program works along with the "ArrowPlasmaTestJava" Java test program (which writes data to Plasma).
  
  - read_OBD.py: Python script which demonstrates reading from an Arrow file; will read data written out by the Java "OBD2Arrow" application. 
  
  - read_PHM08_from_plasma.py: Python script for reading record batches of PHM08 data from Apache Plasma in-memory data store; works with PHM08 data that has been written to Plasma by the Java program "CT2Arrow".

  - read_PHM08_from_plasma_OLD.py: Python script for reading record batches of PHM08 data from Apache Plasma in-memory data store; works with PHM08 data that has been written to Plasma by the Java program "PHM08_to_Plasma".
  
  - write_and_read_example.py: Python script which demonstrates simple example of writing data out to an Arrow file and reading it back in.
  
  - write_and_read_plasma_example.py: Python script which demonstrates simple example of writing an Arrow record batch to a Plasma in-memory object store and then reading it back out from Plasma.

A few notes on using the Plasma in-memory object store
------------------------------------------------------

1. Plasma is only supported on Mac and Linux

2. Must use Python version 3.5+

3. Install PyArrow (https://arrow.apache.org/docs/python/install.html)
     e.g.  pip install pyarrow
     
4. Start up a Plasma store
     e.g.  plasma_store -m 1000000000 -s /tmp/plasma

