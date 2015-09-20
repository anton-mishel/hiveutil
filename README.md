Utility soft for JDBC connectivity to HIVE
Author : Anton Mishel  amishel@outbrain.com


Quick start guide :

1. Check out a repo : 

$git clone https://github.com/anton-mishel/hiveutil.git


2. Adjust hostname you would like to connect to : 

$vi src/main/java/com/java/hiveutil/HiveRemoteCommands.java
Edit a host in DB_URL 
for example : 
private final static String DB_URL = "jdbc:hive2://hiveserver2-nydc1-online.nydc1.outbrain.com:10000/default";



2. Compile :

$cd hiveutil
$mvn clean install

3. Run your code

$cd target 

If you would like to redirect output to STDOUT :

$java -jar hiveutil-1.0-SNAPSHOT-jar-with-dependencies.jar "show tables" 

If you would like to redirect output to a file 

$java -jar hiveutil-1.0-SNAPSHOT-jar-with-dependencies.jar "show tables" "/tmp/result.txt"



