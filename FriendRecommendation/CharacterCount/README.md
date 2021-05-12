# Read Me

## MAP REDUCE PROGRAM 
A java program with Hadoop that count the number of times every unique 5- words occurs in sample text file. This is the exercise that help to run the Hadoop program with java first in ellipse and then in the Hadoop cluster

###  STEPS REQURIED
Create the java Project 
Create the input folder inside the project folder and put the sample file there.
Build the class and write the code there.
Export the project.

#### COMMANDS  
hadoop fs -mkdir -p user/cloudera/input
hadoop fs -put /home/cloudera/Desktop/Sample.txt /user/cloudera/input
hadoop jar /home/cloudera/workspace/myproject.jar WordCount11 /user/cloudera/input /user/cloudera/output
hadoop jar /home/cloudera/Desktop/myproject.jar WordCount11 /user/cloudera/input /user/cloudera/output
hadoop fs -get /user/cloudera/output
