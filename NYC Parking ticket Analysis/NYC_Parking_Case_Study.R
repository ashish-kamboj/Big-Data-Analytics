## Loading Libraries

  library(SparkR)
  library(ggplot2)

## initialise the spark session
  sparkR.session(master='local')

## Create a Spark DataFrame and examine structure

  # reading a CSV file from S3 bucket
    parking_data_2015 <- read.df("s3://ds-big-data/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", source = "CSV", inferSchema = "true", header = "true")

    data_raw <- read.df("s3://data-157/2016/data_2016.csv", source ="csv" , inferSchema = "true" , header ="true")

    parking_2017_data <- SparkR::read.df("s3://bigdata-analysis/spark-casestudy/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", header=T, "CSV")
    
## Examining Structure
    
  # 2015 Data
    head(parking_data_2015)
    printSchema(parking_data_2015)  #11809233
    nrow(parking_data_2015)
    
  # 2016 Data
    nrow(data_raw) #10626899
    ncol(data_raw) #51 columns
    str(data_raw) #
    
  # 2017 Data
    head(parking_2017_data)
    nrow(parking_2017_data) #10803028
    ncol(parking_2017_data) #43
    str(parking_2017_data)
    printSchema(parking_2017_data)
    

    
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##    
############################################################  EDA NYC Parking 2015 Data  ###################################################################################
    
## Checking for the Null values    
    head(filter(parking_data_2015,isNull(parking_data_2015$`No Standing or Stopping Violation`)))
    head(filter(parking_data_2015,isNull(parking_data_2015$`Violation Legal Code`)))
    head(filter(parking_data_2015,isNull(parking_data_2015$Latitude)))
    head(filter(parking_data_2015,isNull(parking_data_2015$Longitude)))
    head(filter(parking_data_2015,isNull(parking_data_2015$`Community Board`)))
    head(filter(parking_data_2015,isNull(parking_data_2015$`Community Council`)))
    head(filter(parking_data_2015,isNull(parking_data_2015$`Census Tract`)))
    head(filter(parking_data_2015,isNull(parking_data_2015$`BIN`)))
    head(filter(parking_data_2015,isNull(parking_data_2015$`BBL`)))
    head(filter(parking_data_2015,isNull(parking_data_2015$`NTA`)))
    
    
## Cleaning the data of the columns that are null through out
  filtered_data<-select(parking_data_2015,parking_data_2015$`Summons Number`,
                          parking_data_2015$`Plate ID`,
                          parking_data_2015$`Registration State`,
                          parking_data_2015$`Plate Type`,
                          parking_data_2015$`Issue Date`, 
                          parking_data_2015$`Violation Code`,
                          parking_data_2015$`Vehicle Body Type`,
                          parking_data_2015$`Vehicle Make`,
                          parking_data_2015$`Issuing Agency`,
                          parking_data_2015$`Street Code1`,
                          parking_data_2015$`Street Code2`,
                          parking_data_2015$`Street Code3`,
                          parking_data_2015$`Vehicle Expiration Date`,
                          parking_data_2015$`Violation Location`,
                          parking_data_2015$`Violation Precinct`,
                          parking_data_2015$`Issuer Precinct`,
                          parking_data_2015$`Issuer Code`,
                          parking_data_2015$`Issuer Command`,
                          parking_data_2015$`Issuer Squad`,
                          parking_data_2015$`Violation Time`,
                          parking_data_2015$`Time First Observed`,
                          parking_data_2015$`Violation County`,
                          parking_data_2015$`Violation In Front Of Or Opposite`,
                          parking_data_2015$`House Number`,
                          parking_data_2015$`Street Name`,
                          parking_data_2015$`Intersecting Street`,
                          parking_data_2015$`Date First Observed`,
                          parking_data_2015$`Law Section`,
                          parking_data_2015$`Sub Division`,
                          parking_data_2015$`Days Parking In Effect    `,
                          parking_data_2015$`From Hours In Effect`,
                          parking_data_2015$`To Hours In Effect`,
                          parking_data_2015$`Vehicle Color`,
                          parking_data_2015$`Unregistered Vehicle?`,
                          parking_data_2015$`Vehicle Year`,
                          parking_data_2015$`Feet From Curb`,
                          parking_data_2015$`Violation Post Code`,
                          parking_data_2015$`Violation Description`)
  
  # Printing the filtered Schema
    printSchema(filtered_data)
    
  # Changing the Issue Date format
    format_data<-withColumn(filtered_data, "Issue Date",unix_timestamp(filtered_data$`Issue Date`,'MM/dd/yyyy') )
    printSchema(format_data)
    
  # Checking for the records having Issue Date other than 2015
    head(summarize((filter(format_data,year(from_unixtime(format_data$`Issue Date`,'yyyy-MM-dd'))!=2015)),count=n(format_data$`Issue Date`)))
    # out of 11809233 parking tickets, 5822402 do not belong to year 2015, so let us ignore them i.e. nearly 50% of the data
    
  # iltering out records wirh Issue Year 2015, on whihc we'll do analysis
    corr_data<-filter(format_data,year(from_unixtime(format_data$`Issue Date`,'yyyy-MM-dd'))==2015)
    
## Removing the spaces with "_" in Column name format
  colnames_df<-colnames(corr_data)
  colnames_df<-gsub(" ","_",colnames_df)
    
  colnames(corr_data)<-colnames_df
  printSchema(corr_data)

## Creating a temporary View
  createOrReplaceTempView(corr_data,"data")
  
  
## Examining the Data
  
  # 1.Find total number of tickets for each year.
    number_of_tickets <- SparkR::sql("SELECT count(distinct Summons_Number) from data")
    head(number_of_tickets)
    
  # 2.Find out how many unique states the cars which got parking tickets came from.
    uniq_reg_state <- SparkR::sql("SELECT count(distinct Registration_State) from data")
    head(uniq_reg_state)     # distinct state count = 68
    
  # 3.Some parking tickets don't have addresses on them, which is cause for concern. Find out how many such tickets there are.
    invalid_addr<-SparkR::sql("SELECT count(*) from data where House_Number is NULL or Street_Name is NULL")
    head(invalid_addr)
    # assumption - house number and street name constitute address
    # total number of invalid address where either house number or street name is null = 888283
    
    
## Aggregation task
  #1. Top 5 violation code
    top_5_vc<-SparkR::sql("SELECT Violation_Code, count(*) violation_count from data where Violation_Code <> 0 group by Violation_Code")
    head(arrange(top_5_vc, desc(top_5_vc$violation_count)))
    
    #   Violation_Code violation_count
    #             21          809914
    #             38          746562
    #             14          517733
    #             36          457313
    #             37          416133
    
    
  #2.a Top 5 vehicle body type
    top_5_vbt<-SparkR::sql("SELECT Vehicle_Body_Type, count(*) typ_count from data where Vehicle_Body_Type is not NULL group by Vehicle_Body_Type")
    head(arrange(top_5_vbt, desc(top_5_vbt$typ_count)))
    
    #Vehicle_Body_Type    typ_count
    #              SUBN   1915458
    #              4DSD   1694252
    #               VAN    879764
    #              DELV    461300
    #               SDN    237304
    
  #2.b. Top 5 vehicle make 
    top_5_vmk<-SparkR::sql("SELECT Vehicle_Make, count(*) mk_count from data where Vehicle_Make is not NULL group by Vehicle_Make")
    head(arrange(top_5_vmk, desc(top_5_vmk$mk_count)))
    
    #  Vehicle_Make mk_count
    #         FORD   763108
    #        TOYOT   619164
    #        HONDA   557315
    #        NISSA   459982
    #        CHEVR   450117
    
  #3.a. Top 5 violating precinct 
    top_5_vp<-SparkR::sql("SELECT Violation_Precinct, count(*) v_count from data where Violation_Precinct<>0 group by Violation_Precinct")
    head(arrange(top_5_vp, desc(top_5_vp$v_count)))
    
    #Violation_Precinct   v_count
    #                 19  320203
    #                 14  217605
    #                 18  215570
    #                  1  169592
    #                114  168458
    
    
  #3.b. Top 5 issuer precinct 
    top_5_ip<-SparkR::sql("SELECT Issuer_Precinct, count(*) i_count from data where Issuer_Precinct<>0 group by Issuer_Precinct")
    head(arrange(top_5_ip, desc(top_5_ip$i_count)))
    
    #Issuer_Precinct   i_count
    #              19  310473
    #              18  212130
    #              14  210516
    #             114  165345
    #               1  165037
    
  #4. # 4.Find the violation code frequency across 3 precincts which have issued the most number of tickets - 
    #do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts?
    
    # Filtering Data based on Top3 Issuer Precinct
    fdata<-filter(corr_data,corr_data$Issuer_Precinct==19 | corr_data$Issuer_Precinct==18 | corr_data$Issuer_Precinct==14)
    
    gdata<-collect(select(fdata,fdata$Issuer_Precinct,fdata$Violation_Code))
    
    # Plotting Issuer Precinct frequency
    ggplot(gdata,aes(x=factor(Issuer_Precinct),fill=factor(Violation_Code)))+geom_bar()
    
    # grouping Issuer Precinct with Violation Code, calculating the counts
    top_v_cd <- SparkR::sql("SELECT Issuer_Precinct,Violation_Code,count(*) tcount from data where Issuer_Precinct in(19,18,14) group by Issuer_Precinct,Violation_Code")
    head(arrange(top_v_cd,desc(top_v_cd$tcount)))
    
    #violation code 14 is highest across all 3 precinct
    #Issuer_Precinct  Violation_Code tcount
    #              18             14  66434
    #              19             38  51710
    #              19             37  45343
    #              14             69  45100
    #              14             14  42633
    #              19             14  34409
    
    
  # 5.You'd want to find out the properties of parking violations across different times of the day:
    
    # a.The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
      # Adding 'M' in the end of the "Voilation Time" to make it in proper format    
        corr_data$Violation_Time_Proper <- concat(corr_data$Violation_Time,lit("M"))
    
      # b.Find a way to deal with missing values, if any.
        # Checking for the not Null values in "Violation Time" field
          corr_data<-filter(corr_data,isNotNull(corr_data$Violation_Time_Proper))
          
      # c.Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion.
        # Converting the Time into proper time format
          corr_data<-withColumn(corr_data,"Violation_Time_Proper",unix_timestamp(corr_data$Violation_Time_Proper,'hhmma'))
      
        # Selecting the Violation Code and extracting hours from the Violation Time
          select_data<-select(corr_data,corr_data$Violation_Code,hour(from_unixtime(corr_data$Violation_Time_Proper,'HH:mm')))
      
        # Renaming the Hours column and adiing into the dataframe
          select_data<-withColumn(select_data,"time_in_hour",select_data$`hour(from_unixtime(Violation_Time_Proper, HH:mm))`)
        head(select_data)
    
        # Creating a Temporary View
          createOrReplaceTempView(select_data,"data")
      
        # Creating the 6 discrete bins for 24-hr time frame
          binning <- SparkR::sql("SELECT Violation_Code, CASE WHEN time_in_hour < 4 then 1
                           when time_in_hour < 8 then 2
                           when time_in_hour < 12 then 3
                           when time_in_hour < 16 then 4
                           when time_in_hour < 20 then 5
                           else 6 end as intervals from data")
      
        # Grouping the Violation code w.r.to the time bining
          x<-summarize(group_by(binning,binning$intervals,binning$Violation_Code),count=n(binning$Violation_Code))
    
        # Creating a temporary View
          createOrReplaceTempView(x,"x_data")
      
          top_3_vc_by_int <- SparkR::sql("select intervals,Violation_Code,count from	
                                   (Select intervals,Violation_Code,count,dense_rank()
                                   OVER(PARTITION BY intervals order by count desc) as rank
                                   from x_data) as tmp
                                   WHERE rank <= 3")
    
        # finding the 3 most commonly occurring violations
          interval_v_c<-collect(select(top_3_vc_by_int,top_3_vc_by_int$intervals,top_3_vc_by_int$Violation_Code,top_3_vc_by_int$count))
          View(interval_v_c)
          interval_v_c
      
        #intervals Violation_Code  count
        #        1             21  21231
        #        1             40  20420
        #        1             78  16413
        #        6              7  32608
        #        6             38  30673
        #        6             14  28364
        #        3             21 643800
        #        3             38 264213
        #        3             36 213760
        #        5             38 126304
        #        5             37  93358
        #        5             14  82682
        #        4             38 323541
        #        4             37 237590
        #        4             36 200027
        #        2             14  76126
        #        2             21  55592
        #        2             40  52012
    
    # 5d. Find the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)
      
      # Filtering the records with most commom occuring Violation Codes
        f<-filter(binning, binning$Violation_Code ==14 | binning$Violation_Code ==28 | binning$Violation_Code ==31)	
        y<-summarize(group_by(f,f$Violation_Code,f$intervals),count=n(f$intervals))							 
    
      # Creating a temporary View
        createOrReplaceTempView(y,"y_data")
    
      top_3_int_by_vc <- SparkR::sql("select Violation_Code,intervals,count from	
                                   (Select intervals,Violation_Code,count,dense_rank()
                                   OVER(PARTITION BY Violation_Code order by count desc) as rank
                                   from y_data) as tmp
                                   WHERE rank <= 3")
    
      # Finding the most common times of day for the 3 most commonly violation codes
      interval_c_v<-collect(select(top_3_int_by_vc,top_3_int_by_vc$Violation_Code,top_3_int_by_vc$intervals,top_3_int_by_vc$count))
      View(interval_c_v)
      interval_c_v
      # Violation_Code intervals  count
      #             31         4  42741
      #             31         3  23666
      #             31         5  16684
      #             28         5      3
      #             28         3      2
      #             28         4      1
      #             14         3 167496
      #             14         4 150872
      #             14         5  82682
    
      
  #6.Let's try and find some seasonality in this data
      
    # a.First, divide the year into some number of seasons, and find frequencies of tickets for each season.
      # Selecting the columns for analysis and storing in another dataframe
        month_data<-select(corr_data,corr_data$Violation_Code,month(from_unixtime(corr_data$Issue_Date,'yyyy-MM-dd')))
        month_data<-withColumn(month_data,"month_in_year",month_data$`month(from_unixtime(Issue_Date, yyyy-MM-dd))`)
        head(month_data)
        
      # Creating a temporary View
      createOrReplaceTempView(month_data,"month_data")
    
      season <- SparkR::sql("SELECT Violation_Code, case when month_in_year=12 or month_in_year=1 or month_in_year=2 then 1
                          when month_in_year=3 or month_in_year=4 or month_in_year=5 then 2
                          when month_in_year=6 or month_in_year=7 or month_in_year=8 then 3
                          when month_in_year=9 or month_in_year=10 or month_in_year=11 then 4 end as seasons from month_data")
    
      # Frequencies of tickets for each season
      freq_season<-summarize(group_by(season,season$seasons),count=n(season$seasons))	
      head(select(freq_season,freq_season$seasons,freq_season$count),5)
      
      #seasons   count
      # 1        2121480
      # 3        1003984
      # 2        2860647
    
  #6.Finding the 3 most common violations for each of these season
    # Summarizing the Violaation code w.r.to seasons
      freq_season<-summarize(group_by(season,season$seasons,season$Violation_Code),count=n(season$Violation_Code))
      
    # Creating a Temporary View
      createOrReplaceTempView(freq_season,"freq_season")
    
      top_3_vc_by_season <- SparkR::sql("select seasons,Violation_Code,count from	
	                        (Select seasons,Violation_Code,count,dense_rank()
	                        OVER(PARTITION BY seasons order by count desc) as rank
	                        from freq_season) as tmp
                          WHERE rank <= 3")
    
    # 3 most common violations for each of these season
      season_vc<-collect(select(top_3_vc_by_season,top_3_vc_by_season$seasons,top_3_vc_by_season$Violation_Code,top_3_vc_by_season$count))
      View(season_vc)
      season_vc
      
      #seasons Violation_Code  count
      #      1             38 312151
      #      1             21 215280
      #      1             14 189939
      #      3             21 169470
      #      3             38 107363
      #      3             36  86115
      #      2             21 425164
      #      2             38 327048
      #      2             14 243624
      
    
  #7.Finding total occurrences of the 3 most common violation codes, No. Of Tickets and Total Fine (or NYC Police revenue)
      
    #violation_code  average fine  no. of tickets  total 
    #  21            55             809914        44545270
    #  38            50             746562        37328100 
    #  14            115            517733        59539295
      
    #14 has the highest fine collected
    
 
      
    
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##      
############################################################  EDA NYC Parking 2016 Data  ###################################################################################

    
## Removinig column having NA columns
  data_raw <- data_raw[,1:38]
    
## Changing Date format 
  data_raw_date <- withColumn(data_raw, "Issue Date", unix_timestamp(data_raw$`Issue Date`,'MM/dd/yyyy'))
  printSchema(data_raw)
    
  head(summarize((filter(data_raw_date,year(from_unixtime(data_raw_date$`Issue Date`,'yyyy-MM-dd'))!=2016)),count=n(data_raw_date$`Issue Date`)))
  # out of 10626899 parking tickets, 5754278 do not belong to year 2015, so let us ignore them i.e. nearly 50% of the data
    
  data_2016 <-filter(data_raw_date,year(from_unixtime(data_raw_date$`Issue Date`,'yyyy-MM-dd')) == 2016)
    

## Examining the Data
    
  # 1) Total Number Of Tickets each Year
    head(select(data_2016 , countDistinct(data_2016$`Summons Number`))) # 4872621
    
  # 2) Find out how many unique states the cars which got parking tickets came from.
    head(select (data_2016 , countDistinct(data_2016$`Registration State`))) # 67 states
    
  # 3) Some parking tickets don't have addresses on them, which is cause for concern. Find out how many such tickets there are.
    nrow(filter(data_2016 , data_2016$`House Number` <= "NA" & data_2016$`Street Name` <= "NA" & data_2016$`Intersecting Street` <= "NA")) 
    # 458106 records do not have addreses
    
    
## Aggregation tasks
    
  # (1) How often does each violation code occur? (frequency of violation codes - find the top 5)
    code_violations <- summarize(groupBy(data_2016, data_2016$`Violation Code`), count = n(data_2016$`Violation Code`))
    
    head(arrange(code_violations, desc(code_violations$count)),5)
    #   Violation Code  count
    #1             21 664947
    #2             36 615242
    #3             38 547080
    #4             14 405885
    #5             37 330489
    
    
  # (2) How often does each vehicle body type get a parking ticket? ------------------------
    body_type <- summarize(groupBy(data_2016, data_2016$`Vehicle Body Type`), count = n(data_2016$`Vehicle Body Type`))
    
    head(arrange(body_type , desc(body_type$count)))
    #  Vehicle Body Type   count
    #1              SUBN 1596326
    #2              4DSD 1354001
    #3               VAN  722234
    #4              DELV  354388
    #5               SDN  178954
    #6              PICK  123258
    
    # How about the vehicle make? (find the top 5 for both)
      body_make <- summarize(groupBy(data_2016, data_2016$`Vehicle Make`), count = n(data_2016$`Vehicle Make`))
    
      head(arrange(body_make , desc(body_make$count)))
    
      #Vehicle Make   count
      #1         FORD 612276
      #2        TOYOT 529115
      #3        HONDA 459469
      #4        NISSA 382082
      #5        CHEVR 339466
      #6        FRUEH 207747
    
  # (3) A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of: 
        #Violating Precincts (this is the precinct of the zone where the violation occurred)
      
    violations_precinct <- summarize(groupBy(data_2016, data_2016$`Violation Precinct`), ount = n(data_2016$`Violation Precinct`))
    
    head(arrange(violations_precinct , desc(violations_precinct$count)))
    #Violation Precinct   count
    #1                  0 1868655
    #2                 19  554465
    #3                 18  331704
    #4                 14  324467
    #5                  1  303850
    #6                114  291336
    
    # Issuing Precincts (this is the precinct that issued the ticket)
      issuer_precint <- summarize(groupBy(data_2016 , data_2016$`Issuer Precinct`),count =n(data_2016$`Issuer Precinct`))
    
      head(arrange(issuer_precint , desc(issuer_precint$count)))
      # Issuer Precinct   count
      #     0             828348
      #     19            264299
      #     13            156144
      #     1             152231
      #     14            150637
      #     18            148843
    
    
  # (4) Find the violation code frequency across 3 precincts which have issued the most number of tickets 
    #do these precinct zones have an exceptionally high frequency of certain violation codes? Every issuer precinct has a hihg number of violation code 38   
    # Are these codes common across precincts? 
    
    # Filtering the data based on tp3 Issuer Precinct
    Top_Precincts <- filter(data_2016 , data_2016$`Issuer Precinct` <= 0 | data_2016$`Issuer Precinct` <= 19 | data_2016$`Issuer Precinct` <= 13)
    
    # Creating a temporary View
    createOrReplaceTempView(Top_Precincts,"tbl_top_precincts")
    
    top_v_issuer <- SparkR::sql("SELECT `Issuer Precinct`,`Violation Code`,count(*) tcount 
                                from tbl_top_precincts 
                                where `Issuer Precinct` in(10,19,13)
                                group by `Issuer Precinct`,`Violation Code`")
    
    head(arrange(top_v_issuer , desc(top_v_issuer$tcount)),20)
    
    #         Issuer Precinct    Violation Code   tcount
    #1               19             37            38052
    #2               19             38            37855
    #3               19             46            36442
    #4               19             14            28772
    #5               19             21            25588
    #6               19             16            24647
    #7               13             69            23356
    #8               13             47            17532
    #9               13             38            16447
    #10              13             14            15812
    #11              19             20            14085
    #12              13             37            13589
    #13              13             31            13204
    #14              10             14            12122
    #15              19             40            9474
    #16              13             42            8988
    #17              19             71            6608
    #18              19             19            6561
    #19              10             38            6099
    #20              19             10            5713
    
    
  # (5) You'd want to find out the properties of parking violations across different times of the day:
    
    #(5.A) The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
      data_2016 <-  withColumn(data_2016 , "v_time" ,substr(data_2016$`Violation Time`,1,4))
      data_2016 <-  withColumn(data_2016 , "AM/PM" ,substr(data_2016$`Violation Time`,6,6))
    
      data_2016$v_time_new <- ifelse(data_2016$`AM/PM` == "P" & data_2016$v_time < 1200 , data_2016$v_time+1200 ,data_2016$v_time)
      data_2016 <-withColumn(data_2016,"Hour" ,substr(data_2016$v_time_new,1,2)) 
    
    # Find a way to deal with missing values, if any. #NO Miising Or Null Value
      count(filter(data_2016 , data_2016$`Violation Time` == "NA"))
      count(filter(data_2016 , data_2016$`Violation Time` == ""))
      count(filter(data_2016 , data_2016$`Violation Time` == NULL))
    
    #(5.B)  Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. 
      data_2016 <- withColumn(data_2016, "Hour", cast(data_2016$Hour, "double"))
      
      # Creating a temporary View
      createOrReplaceTempView(data_2016,"tbl_2016")
    
      Time_bins <- SparkR::sql("SELECT `Violation Code`, Hour, CASE WHEN Hour < 04 then 'Late_Night'
                             when Hour < 08 then 'Early_Morning'
                             when Hour < 12 then  'Morning'
                             when Hour < 16 then  'Afternoon'
                             when Hour < 20 then 'Evening'
                             else 'Night' end as intervals from tbl_2016")
    
      sum_bin<-  summarize(groupBy(Time_bins , Time_bins$intervals , Time_bins$`Violation Code`) , count = n(Time_bins$`Violation Code`))
    
      # Creating a Temporary View
      createOrReplaceTempView(sum_bin,"data_bins")
    
    #(5.C) For each of these groups, find the 3 most commonly occurring violations
    
      top_3_int_vio <- SparkR::sql("select intervals,`Violation Code`,count from	
                                 (Select intervals,`Violation Code`,count,dense_rank()
                                 OVER(PARTITION BY intervals order by count desc) as rank
                                 from data_bins) as tmp
                                 WHERE rank <= 3")
    
      inter_vio_sum <-collect(select(top_3_int_vio,top_3_int_vio$intervals,top_3_int_vio$`Violation Code`,top_3_int_vio$count))
      inter_vio_sum
    
      #         intervals       Violation Code  count
      #1        Evening             38          105657
      #2        Evening             37          79991
      #3        Evening             14          63778
      #4        Morning             21          525281
      #5        Morning             36          284279
      #6        Morning             38          185395
      #7        Late_Night          21          29884
      #8        Late_Night          40          17033
      #9        Late_Night          78          13282
      #10       Afternoon           36          273581
      #11       Afternoon           38          234247
      #12       Afternoon           37          183864
      #13       Early_Morning       14          65347
      #14       Early_Morning       21          48240
      #15       Early_Morning       40          42307
      #16       Night               38          20851
      #17       Night               7           20246
      #18       Night               40          20038
    
    # (5.D) Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)
    
    # Filtering the data based on the top3 Violation Code
    Vio_bins <-filter(Time_bins, Time_bins$`Violation Code` == 36 | Time_bins$`Violation Code` == 07 | Time_bins$`Violation Code` == 21)	
    vio_sum<-summarize(group_by(Vio_bins,Vio_bins$`Violation Code`,Vio_bins$intervals),count=n(Vio_bins$intervals))							 
    
    # Creating a temporary View
    createOrReplaceTempView(vio_sum,"tbl_vio_sum")
    
    top_3_vio_sum <- SparkR::sql("select `Violation Code`,intervals,count from	
                                 (Select intervals,`Violation Code`,count,dense_rank()
                                 OVER(PARTITION BY `Violation Code` order by count desc) as rank
                                 from tbl_vio_sum) as tmp1
                                 WHERE rank <= 3")
    
    vio_inter_sum <-collect(select(top_3_vio_sum,top_3_vio_sum$`Violation Code`,top_3_vio_sum$intervals,top_3_vio_sum$count))
    vio_inter_sum
    
    #  Violation Code     intervals       count
    #1              7     Afternoon       149460
    #2              7     Evening         124617
    #3              7     Morning         100390
    #4             21     Morning         1209244
    #5             21     Afternoon       138641
    #6             21     Early_Morning   114029
    #7             36     Morning         586791
    #8             36     Afternoon       545717
    #9             36     Early_Morning   79797
    
    
  # (6) Let's try and find some seasonality in this data
    
    # (6.A) First, divide the year into some number of seasons, and find frequencies of tickets for each season.
      month_data_2016<-select(data_2016 , data_2016$`Violation Code` , month(from_unixtime(data_2016$`Issue Date`,'yyyy-MM-dd') ) )
      colnames(month_data_2016)[2] <-"month"
    
      month_data_2016<-withColumn(month_data_2016,"month_in_year",month_data_2016$month)
    
      head(month_data_2016)
      #Violation Code   month month_in_year
      #     24            6             6
      #     40            6             6
      #     67            6             6
      #     20            6             6
      #     21            7             7
      #     98            6             6
    
    # Creating a temporary View
    createOrReplaceTempView(month_data_2016,"tbl_month_data")
    
    season_2016 <- SparkR::sql("SELECT `Violation Code`, case when month_in_year=12 or month_in_year=1 or month_in_year=2 then 'Winter'
                               when month_in_year=3 or month_in_year=4 or month_in_year=5 then 'Spring'
                               when month_in_year=6 or month_in_year=7 or month_in_year=8 then 'Summer'
                               when month_in_year=9 or month_in_year=10 or month_in_year=11 then 'Autumn' end as seasons from tbl_month_data")
    
    # Calculating the seasons frequency
    freq_season_2016 <-summarize(group_by(season_2016,season_2016$seasons),count=n(season_2016$seasons))	
    
    head(select(freq_season_2016 ,freq_season_2016$seasons,freq_season_2016$count),5)
    
    #   Season  Count
    #1  Spring 2789066
    #2  Summer  427796
    #3  Autumn     903
    #4  Winter 1654856
    
    
    # (6.B) Then, find the 3 most common violations for each of these season  
    
    freq_season_2016 <- summarize(group_by(season_2016,season_2016$seasons,season_2016$`Violation Code`) ,count=n(season_2016$`Violation Code`))	
    
    # Creating a temporary View
    createOrReplaceTempView(freq_season_2016,"tbl_freq_season")
    
    top_3_vc_ssn<- SparkR::sql("select seasons,`Violation Code`,count from	
                               (Select seasons,`Violation Code`,count,dense_rank()
                               OVER(PARTITION BY seasons order by count desc) as rank
                               from tbl_freq_season)
                               as tmp2
                               WHERE rank <= 3")
    
    season_vc_2016 <-collect(select(top_3_vc_ssn,top_3_vc_ssn$seasons,top_3_vc_ssn$`Violation Code`,top_3_vc_ssn$count))
    season_vc_2016
    
    #seasons Violation Code  count
    #1   Spring             21 383448
    #2   Spring             36 374362
    #3   Spring             38 299439
    #4   Summer             21  60007
    #5   Summer             38  51077
    #6   Summer             14  45020
    #7   Autumn             46    195
    #8   Autumn             21    192
    #9   Autumn             40     79
    #10  Winter             21 221300
    #11  Winter             36 200971
    #12  Winter             38 196559
    
    
  # (7) The fines collected from all the parking violation constitute a revenue source for the NYC police department. 
    #Let's take an example of estimating that for the 3 most commonly occurring codes.
    
    #(7.A)Find total occurrences of the 3 most common violation codes
    
    #   Violation Code  count
    #1             21   664947
    #2             36   615242
    #3             38   547080
    
    # (7.B) Then, search the internet for NYC parking violation code fines. You will find a website (on the nyc.gov URL) that lists these fines.
      #They're divided into two categories, one for the highest-density locations of the city, the other for the rest of the city. 
      #For simplicity, take an average of the two.
    
    
    #     CODE	DEFINITION                                                               	Manhattan96th St. & below         	All Other Areas  AVG  FINE
    #     21	Street Cleaning: No parking where parking is not allowed by sign.                     	$65                     	$45                 55
    #             , street marking or traffic control device
    
    
    #     36    Exceeding the posted speed limit in or near a designated school zone.	                $50	                      $50                 50                
    
    
    #     38	Muni Meter Failing to show a receipt or tag in the windshield.                         	$65                     	$35                 50
    #         Drivers get a 5-minute grace period past the expired time on Muni-Meter receipts.
    
    
    #Using this information, find the total amount collected for all of the fines. State the code which has the highest total collection.
    #   Violation Code    count       AVG FINE      TOTAL FINE 
    #1             21     664947        55            36572085
    #2             36     615242        50            30762100  
    #3             38     547080        50            27354000
    
    #Code 21 has generated the Revenue for the police dept
    
    
    
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##      
############################################################  EDA NYC Parking 2017 Data  ###################################################################################

## Checking for the columns with null or NA values
  count(filter(parking_2017_data, parking_2017_data$`Street Code1` =="0"))  #2642599
  count(filter(parking_2017_data, parking_2017_data$`Street Code2` =="0"))  #3413866
  count(filter(parking_2017_data, parking_2017_data$`Street Code3` =="0"))  #3523028
  count(filter(parking_2017_data, parking_2017_data$`Vehicle Expiration Date` =="0"))   #2432921
  count(filter(parking_2017_data, isNull(parking_2017_data$`Violation Location`)))  #2072341
  count(filter(parking_2017_data, isNull(parking_2017_data$`Issuer Command`)))  #2062645
  count(filter(parking_2017_data, isNull(parking_2017_data$`Issuer Squad`)))  #2063541
  count(filter(parking_2017_data, isNull(parking_2017_data$`Time First Observed`)))   #9962218
  count(filter(parking_2017_data, isNull(parking_2017_data$`Violation In Front Of Or Opposite`)))   #2161173
  count(filter(parking_2017_data, isNull(parking_2017_data$`House Number`)))  #2288556
  count(filter(parking_2017_data, parking_2017_data$`Date First Observed` =="0"))   #10561435
  count(filter(parking_2017_data, isNull(parking_2017_data$`Days Parking In Effect    `)))  #2712412
  count(filter(parking_2017_data, isNull(parking_2017_data$`From Hours In Effect`)))  #5450942
  count(filter(parking_2017_data, isNull(parking_2017_data$`To Hours In Effect`)))  #5450939
  count(filter(parking_2017_data, isNull(parking_2017_data$`Unregistered Vehicle?`)))   #9675428
  count(filter(parking_2017_data, isNull(parking_2017_data$`Meter Number`)))  #9017493
  count(filter(parking_2017_data, parking_2017_data$`Feet From Curb` =="0")) #10531742
  count(filter(parking_2017_data, isNull(parking_2017_data$`Violation Post Code`))) #3190182
  count(filter(parking_2017_data, isNull(parking_2017_data$`No Standing or Stopping Violation`))) #10802965
  count(filter(parking_2017_data, isNull(parking_2017_data$`Hydrant Violation`))) #10802965
  count(filter(parking_2017_data, isNull(parking_2017_data$`Double Parking Violation`))) #10802965

      
## Removing the columns having null or NA values as observed above
    
  parking_2017 <- select(parking_2017_data, parking_2017_data$`Summons Number`, parking_2017_data$`Plate ID`, parking_2017_data$`Registration State`,
                           parking_2017_data$`Plate Type`,parking_2017_data$`Issue Date`,parking_2017_data$`Violation Code`,parking_2017_data$`Vehicle Body Type`,
                           parking_2017_data$`Vehicle Make`,parking_2017_data$`Issuing Agency`,parking_2017_data$`Violation Precinct`,parking_2017_data$`Issuer Precinct`,
                           parking_2017_data$`Issuer Code`,parking_2017_data$`Violation Time`,parking_2017_data$`Violation County`,parking_2017_data$`House Number`,
                           parking_2017_data$`Street Name`,parking_2017_data$`Intersecting Street`,parking_2017_data$`Law Section`,parking_2017_data$`Sub Division`,
                           parking_2017_data$`Violation Legal Code`,parking_2017_data$`Vehicle Color`,parking_2017_data$`Violation Description`)
    
## Filtering the records based on the Issue Date (Ticket Issue Year should be 2017 only)
    
  # Extracting year from the "Issue Date"
    parking_2017_data <- withColumn(parking_2017_data, 'Year',substr(parking_2017_data$`Issue Date`,8, 11))
    
  # Filtering the records having Issue year other than 2017
    parking_2017 <- filter(parking_2017_data,parking_2017_data$Year == 2017)
    
  # Counts of records having ticket issuing year=2017 from the total records
    count(filter(parking_2017_data,parking_2017_data$Year == 2017)) #5431918
    
## Examining the data
    
  # 1.Find total number of tickets for each year
    total_number_of_tickets <- head(select(parking_2017, countDistinct(parking_2017$`Summons Number`)))
    total_number_of_tickets  #10803028
    
  # 2.Find out how many unique states the cars which got parking tickets came from
    unique_states <- count(distinct(parking_2017[,"`Registration State`"]))  #65
    
  # 3.Some parking tickets don't have addresses on them, which is cause for concern. Find out how many such tickets there are
    
    count(filter(parking_2017, isNull(parking_2017$`House Number`) & isNull(parking_2017$`Street Name`) & isNull(parking_2017$`Intersecting Street`))) #1312
    # I'm considering here Address is a combination of 'House Number', 'Street Name' and 'Intersecting Street', if all there are null, 
    # then considering as null or empty address (meand address doen't present)
    
    
## Aggregation tasks
    
  # 1.How often does each violation code occur? (frequency of violation codes - find the top 5)
    violation_code_counts <- summarize(groupBy(parking_2017, parking_2017$`Violation Code`),count = n(parking_2017$`Violation Code`))
    head(arrange(violation_code_counts, desc(violation_code_counts$count)),5)
    
    # Violation Code    count
    #     21            768087
    #     36            662765
    #     38            542079
    #     14            476664
    #     20            319646
    
    
  # 2.How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)
    
    # Parking tickets summary w.r.to vehicle body
      vechicle_body_type_counts <- summarize(groupBy(parking_2017, parking_2017$`Vehicle Body Type`),count = n(parking_2017$`Vehicle Body Type`))
      head(arrange(vechicle_body_type_counts, desc(vechicle_body_type_counts$count)),5)
    
      #Vechicle Body Type     Count
      #     SUBN              1883954
      #     4DSD              1547312
      #     VAN               724029
      #     DELV              358984
      #     SDN               194197
    
    # Parking tickets summary w.r.to vehicle Make
      vechicle_make_counts <- summarize(groupBy(parking_2017, parking_2017$`Vehicle Make`),count = n(parking_2017$`Vehicle Make`))
      head(arrange(vechicle_make_counts, desc(vechicle_make_counts$count)),5)
    
      # Vechicle make      Count
      #   FORD            636844
      #   TOYOT           605291
      #   HONDA           538884
      #   NISSA           462017
      #   CHEVR           356032
    
      
  # 3.A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of:
    
    #a. Violating Precincts (this is the precinct of the zone where the violation occurred)
      # Filtering the records with Violation Precinct=0
        violation_precincts <- filter(parking_2017,parking_2017$`Violation Precinct` != "0")
    
        violation_precincts_counts <- summarize(groupBy(violation_precincts, violation_precincts$`Violation Precinct`),count = n(violation_precincts$`Violation Precinct`))
        head(arrange(violation_precincts_counts, desc(violation_precincts_counts$count)),5)
    
        # Violation Precinct    count
        #         19            274445
        #         14            203553
        #         1             174702
        #         18            169131
        #         114           147444
    
    #b. Issuing Precincts (this is the precinct that issued the ticket)
      # Filtering the records with Issuer Precinct=0
        issuer_precinct <- filter(parking_2017,parking_2017$`Issuer Precinct` != "0")
    
        issuer_precinct_counts <- summarize(groupBy(issuer_precinct, issuer_precinct$`Issuer Precinct`),count = n(issuer_precinct$`Issuer Precinct`))
        head(arrange(issuer_precinct_counts, desc(issuer_precinct_counts$count)),5)
    
        # Issuer Precinct   count
        #       19          266961
        #       14          200495
        #       1           168740
        #       18          162994
        #       114         144054
    
  # 4.Find the violation code frequency across 3 precincts which have issued the most number of tickets - 
    #do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts?
    
    precincts_subset <- filter(parking_2017, ("`Issuer Precinct` in ('19','14','1')"))
    
    violation_accross_precinct_counts <- summarize(groupBy(precincts_subset, precincts_subset$`Violation Code`),count = n(precincts_subset$`Violation Code`))
    head(arrange(violation_accross_precinct_counts, desc(violation_accross_precinct_counts$count)))
    
  # 5.You'd want to find out the properties of parking violations across different times of the day:
    
    # a.The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
      # Adding 'M' in the end of the "Voilation Time" to make it in proper format                                                                                                   
        parking_2017$`Violation Time` <- concat(parking_2017$`Violation Time`,lit("M"))
    
    # b.Find a way to deal with missing values, if any.
      # Checking for the Null values in "Violation Time" field
        count(filter(parking_2017, isNull(parking_2017$`Violation Time`)))
    
      # Filtering the Non-null values
        parking_2017<-filter(parking_2017,isNotNull(parking_2017$`Violation Time`))
    
    # c.Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion.
    
      # Extracting the hours from the "Violation Time"
        parking_2017<-withColumn(parking_2017,"Violation Time Proper",unix_timestamp(parking_2017$`Violation Time`,'hhmma'))
        violation_time_data<-select(parking_2017,parking_2017$`Violation Code`,hour(from_unixtime(parking_2017$`Violation Time Proper`,'HH:mm')))
        violation_time_data<-withColumn(violation_time_data,"Time_In_Hour",violation_time_data$`hour(from_unixtime(Violation Time Proper, HH:mm))`)
        head(violation_time_data)
    
      # Creating a temporary view             
        createOrReplaceTempView(violation_time_data,"violation_code_time_data")
    
      # Creating the 6 discrete bins for 24-hr time frame
        voilation_time_binning <- SparkR::sql("select `Violation Code`, 
                                          case when Time_In_Hour <=4 then 1
                                          when (Time_In_Hour >4 and Time_In_Hour <=8) then 2
                                          when (Time_In_Hour >8 and Time_In_Hour <=12) then 3
                                          when (Time_In_Hour >12 and Time_In_Hour <=16) then 4
                                          when (Time_In_Hour >16 and Time_In_Hour <=20)  then 5
                                          else 6
                                          end as intervals 
                                          from violation_code_time_data")
    
      # Grouping the Violation code w.r.to the time bining
        voilation_code_time_summary<-summarize(group_by(voilation_time_binning,voilation_time_binning$intervals,voilation_time_binning$`Violation Code`),
                                           count=n(voilation_time_binning$`Violation Code`)) # 21, 36,38
    
      # Creating a temporary view
        createOrReplaceTempView(voilation_code_time_summary,"top_3_voilation_data")
    
        top_3_vc_by_int <- SparkR::sql("select intervals,`Violation Code`,count from	
                                   (Select intervals,`Violation Code`,count,dense_rank()
                                   OVER(PARTITION BY intervals order by count desc) as rank
                                   from top_3_voilation_data) as tmp
                                   where rank <= 3")
    
      # finding the 3 most commonly occurring violations
        interval_v_c<-collect(select(top_3_vc_by_int,top_3_vc_by_int$intervals,top_3_vc_by_int$`Violation Code`,top_3_vc_by_int$count))
        View(interval_v_c)
        interval_v_c
    
    # d. Find the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)
    
      # Filtering the records with most commom occuring Violation Codes
        f<-filter(voilation_time_binning, voilation_time_binning$`Violation Code` ==21 | voilation_time_binning$`Violation Code` ==36 | 
                voilation_time_binning$`Violation Code` ==38)	
        y<-summarize(group_by(f,f$`Violation Code`,f$intervals),count=n(f$intervals))							 
    
      # Creating a temporary view
        createOrReplaceTempView(y,"y_data")
    
        top_3_int_by_vc <- SparkR::sql("select `Violation Code`,intervals,count from	
                                   (Select intervals,`Violation Code`,count,dense_rank()
                                   OVER(PARTITION BY `Violation Code` order by count desc) as rank
                                   from y_data) as tmp
                                   WHERE rank <= 3")
    
      # Finding the most common times of day for the 3 most commonly violation codes
        interval_c_v<-collect(select(top_3_int_by_vc,top_3_int_by_vc$`Violation Code`,top_3_int_by_vc$intervals,top_3_int_by_vc$count))
        View(interval_c_v)
        interval_c_v
    
  #6. Let's try and find some seasonality in this data
    
    # a.First, divide the year into some number of seasons, and find frequencies of tickets for each season.
      # Extracting month from the "Issue Date" and Renaming the extracted month Column
        parking_2017 <- withColumn(parking_2017, 'Month_In_Year',substr(parking_2017$`Issue Date`,2, 3))
    
      # Selecting the columns for analysis and storing in another dataframe
        month_data<-select(parking_2017,parking_2017$`Violation Code`,parking_2017$Month_In_Year)
        head(month_data)
    
      # Creating temporary view
        createOrReplaceTempView(month_data,"month_data")
    
        season <- SparkR::sql("SELECT `Violation Code`, 
                          case when month_in_year=12 or month_in_year=1 or month_in_year=2 then 1
                          when month_in_year=3 or month_in_year=4 or month_in_year=5 then 2
                          when month_in_year=6 or month_in_year=7 or month_in_year=8 then 3
                          when month_in_year=9 or month_in_year=10 or month_in_year=11 then 4 
                          end as seasons 
                          from month_data")
    
      # Frequencies of tickets for each season
        freq_season<-summarize(group_by(season,season$seasons),count=n(season$seasons))	
        head(select(freq_season,freq_season$seasons,freq_season$count),5)
    
    # b.Finding the 3 most common violations for each of these season
      # Summarizing the Violaation code w.r.to seasons
        freq_season<-summarize(group_by(season,season$seasons,season$`Violation Code`),count=n(season$`Violation Code`))
    
      # Creating a temporary view
        createOrReplaceTempView(freq_season,"freq_season")
    
        top_3_vc_by_season <- SparkR::sql("select seasons,`Violation Code`,count from	
                                  (Select seasons,`Violation Code`,count,dense_rank()
                                  OVER(PARTITION BY seasons order by count desc) as rank
                                  from freq_season) as tmp
                                  where rank <= 3")
    
      # 3 most common violations for each of these season
        season_vc<-collect(select(top_3_vc_by_season,top_3_vc_by_season$seasons,top_3_vc_by_season$`Violation Code`,top_3_vc_by_season$count))
        View(season_vc)
        season_vc
    
  #7 Finding total occurrences of the 3 most common violation codes, No. Of Tickets and Total Fine (or NYC Police revenue)
    
    # Voilation code    No. of Tickets    Average Fine    Total Fine
    #   21                  768087            55            42244785
    #   36                  662765            50            33138250
    #   38                  542071            50            27103550
    
    #Code 21 has generated the Revenue for the police dept
    
