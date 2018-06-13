## Problem Statement:
New York City is a thriving metropolis. Just like most other metros that size, one of the biggest problems its citizens face, is parking. The classic combination of a huge number of cars, and a cramped geography is the exact recipe that leads to a huge number of parking tickets. In an attempt to scientifically analyse this phenomenon, the NYC Police Department has collected data for parking tickets. 

Need to compare phenomenon related to parking tickets over three different years - 2015, 2016, 2017.


## Uploading data into S3:
**Dataset** used for analysis is vailable on Kaggle. Link- [https://www.kaggle.com/new-york-city/nyc-parking-tickets/data]
**Data Dictonary** - [https://www.kaggle.com/new-york-city/nyc-parking-tickets/data]

Below are the steps to upload the kaggle dataset into S3-

1. First copied cookie information for kaggle site in a text file.
2. Transfered the cookies file to the EC2 instance using the command [scp -i /path/my-key-pair.pem /path/cookies.txt user-name@ec2-xxx-xx-xxx-x.compute-1.amazonaws.com:~]. We can also copy the file using WinSCP.
3. Connected to the Master node using SSH through PuTTY and PuTTYGen
4. From the terminal used the belowcommand to download the dataset into the EC2 instance.
wget -x --load-cookies cookies.txt https://www.kaggle.com/new-york-city/nyc-parking-tickets/downloads/Parking_Violations_Issued_-_Fiscal_Year_2015.csv/2
wget -x --load-cookies cookies.txt https://www.kaggle.com/new-york-city/nyc-parking-tickets/downloads/Parking_Violations_Issued_-_Fiscal_Year_2016.csv/2
wget -x --load-cookies cookies.txt https://www.kaggle.com/new-york-city/nyc-parking-tickets/downloads/Parking_Violations_Issued_-_Fiscal_Year_2017.csv/2

5. Uploaded the data to S3 via command line 
 * aws s3 cp Parking_Violations_Issued_-_Fiscal_Year_2015.csv s3://bigdata-analytics/spark-analysis/2015/
 * aws s3 cp Parking_Violations_Issued_-_Fiscal_Year_2016.csv s3://bigdata-analytics/spark-analysis/2016/
 * aws s3 cp Parking_Violations_Issued_-_Fiscal_Year_2017.csv s3://bigdata-analytics/spark-analysis/2017/


## Exploratory Data Analysis Using Apache Spark:
1. Dataset used: Year- 2015, 2016, 2017
2. Spark allowed to analyse the full files at high speeds
3. Libraries Used - SparkR, ggplot2

### Examine the data:

1. Total number of tickets for each year.
2. How many unique states the cars which got parking tickets came from?
3. How many tickets without addresses?
 
### Aggregation tasks:

1. How often does each violation code occur? (frequency of violation codes - top 5)
2. How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)
3. A precinct is a police station that has a certain zone of the city under its command. Found the (5 highest) frequencies of:
	- Violating Precincts (this is the precinct of the zone where the violation occurred)
	- Issuing Precincts (this is the precinct that issued the ticket)
4. Violation code frequency across 3 precincts which have issued the most number of tickets (Also checked those codes common across precincts).
5. Properties of parking violations across different times of the day:
	- The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
	- dealt with missing values
	- Divided 24 hours into 6 equal discrete bins of time and fond the 3 most commonly occurring violations
	- For the 3 most commonly occurring violation codes, found the most common times of day (in terms of the bins created above)

6. Seasonality in the data
	- Divided the year into some number of seasons, and found the frequencies of tickets for each season.
	- found the 3 most common violations for each of these season

7. The fines collected from all the parking violation constitute a revenue source for the NYC police department. 		- 
	- Total occurrences of the 3 most common violation codes
	- Estimated the revenue for the 3 most commonly occurring codes (used the fines provided on nyc.gov)
	- Total amount collected for all of the fines. Stated the code which has the highest total collection.

