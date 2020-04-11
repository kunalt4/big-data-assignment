# Big Data Assignment

## Bash for Big Data Analysis

Files for this section are present in the 1_Bash folder. We assume the commands are being run in this folder.

*1. How many lines of content (no header) is there in the file? (tail, wc)*
        
File: 1_lines_in_file.sh

*2. Create a bash command (using, among other things, sed and wc) to count the number of columns*
        
File: 2_number_of_columns.sh

*3. For a given city (given as a column number, e.g., 10=Sydney), what is the type of crime on top of the crime list (cat, cut, sort, head)?*
        
File: 3_crime_city_top.sh<br>
Command: `./3_crime_city_top <column_number>`

*4. Find the number of crimes for a given city (given as a column number, e.g., 10=Sydney): create a bash script that reads all the rows (see previous question) and sums up the crime values*
        
File: 4_total_crime_city.sh<br>
Command: `./4_total_crime_city.sh <column_number>`

*5. Same question with the average - look at question 1 to get the number of rows & use tr to remove the empty white spaces and get the number*

File: 5_average_crime_city.sh<br>
Command: `./5_average_crime_city.sh <column_number>`
   
*6. Get the city with the lowest average crime. Create a bash scripts that goes through all the cities, compute the average crime rate and keep only the city with the lowest value. Finally display the city and the average number of crimes.*

File: 6_average_crime_across_cities.sh

------------------------------------------------------------------------------------------------------------------------------------------------------

## Data Management

Files for this section are present in the 2_Data_Management folder. We assume the commands are being run in this folder.
********************************************************************************************************************

**Populating Databases:**

These scripts are present in the Populate_DBs folder.

***1. Populating MySQL using bash script:***

The script mysql_import.sh populates the MySQL database, into the database players_teams_positions.
Two required arguments are needed to run the file - 1. mysql username, 2. mysql password.

Command: `./mysql_import.sh <mysql_username> <mysql_password>`

The Players.csv and Teams.csv files must be present in the same folder as the bash script.
A positions.csv file will be created using Players.csv and used by the script to populate the Positions table.

WARNING: If a database named 'players_teams_positions' exists, it will be deleted and a new one will be created.

Three tables are populated - Player_details, Team_details and Positions.

***2. Populating MongoDB using bash script:***

The script mongo_import.sh populates a MongoDB Collection in the database for each csv file passed. 

Command: `./mongo_import.sh <database_name> <csv_file1> [<csv_file2> <csv_file3> ...]`

A minimum of two arguments are required to run the file - 
1. Database name
2. csv file names separated by space
   
A collection is formed for each csv file passed, with the collection name the same as that of each file.

********************************************************************************************************************

**Running Queries:**

These files are present in the Queries folder inside 2_Data_Management.

***1. MySQL Queries:***

mysql_queries.sql - This script will run all the MySQL Queries - Q1 to Q8<br>
Command: `mysql -u <username> -p <database_name> < mysql_queries.sql`

***2. MongoDB Queries***

mongo_queries.js -  This script will run all the MongoDB Queries - Q1 to Q8<br>
Command: `mongo <database_name> < mongo_queries.js`

------------------------------------------------------------------------------------------------------------------------------------------------------


## Hadoop

These files are present in the 4_Hadoop folder.

This folder consists of Java files for Q1 **(Calculating Average Number of Passengers Per Trip in general and per day of week)**, Q2 **(Calculating Trip Distance Per Trip in general and per day of week)**, Q4 **(Calculating Average Number of Passengers per hour, overall and based on weekdays and weekends)**, and Q5. **(Calculating Average NumTrip Distance per hour, overall and based on weekdays and weekends)**
Along with this, a bash script for Q3 **(Most used payment types)** is present.

The yellow_tripdata_2019-01.csv file is not present, but should be passed to the bash script for Q3.

Hadoop must be setup on either a container, your local machine or on a VM.
The files must be run from the namenode.
The file yellow_tripdata_2019-01.csv must be present on the namenode.

Accessing namenode and creating input:

1. `docker exec -it namenode bash`<br>
2. `hdfs dfs -mkdir /<input_directory>`<br>
3. `hdfs dfs -copyFromLocal yellow_tripdata_2019-01.csv /<input_directory>`<br><br>

This will create an input_directory on the dfs, and copy the data file onto it.

Running the job:

`export HADOOP_CLASSPATH=/usr/lib/jvm/java-1.8.0-openjdk-amd64/lib/tools.jar` (export hadoop classpath)<br>
`hadoop com.sun.tools.javac.Main <filename>.java (compile java file)`<br>
`jar cf <jar_name>.jar <filename>*.class (create jar file)`<br>
`hadoop jar <jar_name>.jar <filename> /input /<output_directory> (run mapreduce job)`<br><br>


The output_directory should not be already present.

To check the output:


`hdfs dfs -cat /<output_directory>/part-r-00000`



The questions and their respective java files are:

Q1 - AvgPassenger_Q1.java<br>
Q2 - AvgTripDistance_Q2.java<br>
Q4 - AvgPassenger_Q4A.java (Overall), AvgPassenger_Q4B.java (Weekdays and Weekends)<br>
Q5 - AvgTripDistance_Q5A.java (Overall), AvgTripDistance_Q5B.java (Weekdays and Weekends)


For Q3, a bash script is used. The yellow_tripdata_2019-01.csv file needs to be passed as argument.
File: most_payment_type.sh

Command: `./most_payment_type.sh <filename>`

------------------------------------------------------------------------------------------------------------------------------------------------------

