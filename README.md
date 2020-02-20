Tasks Scheduler:

Input files are located in the resources folder.
Main Driver program takes all the inputs required for Scheduling.Program cane be run in 2 ways.

1)Running the Main file.

2)Using spark submit:




**Compiling**


mvn compile


**Running**

mvn assembly:single

**bash**

spark-submit --class Main --master local target/schedulingtasks-1.0-SNAPSHOT-jar-with-dependencies.jar 

