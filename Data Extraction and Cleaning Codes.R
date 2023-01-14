
library(httr)
library(jsonlite)

#Function to Extract Accident Dataset using API
accidentdata_api <-function()
{

  res = GET("https://ckan.publishing.service.gov.uk/api/action/package_search?q=road+safety")
  data = fromJSON(rawToChar(res$content))
  url <- data$result$results[[2]]$resources[[21]]$url 
  data <- read.csv2(url,sep = ",")
  return(data)
  
}


#Function to Extract Vehicle Dataset using API
vehicledata_api <-function()
{
  
  res = GET("https://ckan.publishing.service.gov.uk/api/action/package_search?q=road+safety")
  data = fromJSON(rawToChar(res$content))
  url <- data$result$results[[2]]$resources[[22]]$url 
  data <- read.csv2(url,sep = ",")
  return(data)
  
}


#mm <-read.csv("Merged_MSOA.csv")
# setwd("C:/Users/LabStudent-55-706949/Downloads/dataset/MSOA")
# setwd("C:/Users/LabStudent-55-706949/Downloads/dataset")


#Function to Extract All Other Datasets from CSV Files

data_fetch <- function()
{  
  
  setwd("C:/Users/jazzj/Downloads/DQ/dataset")
  lsoa <- read.csv("Lower_Layer_Super_Output_Area__2011__to_Ward__2019__Lookup_in_England_and_Wales.csv")
  imd <- read.csv("IMD_2019.csv") 
  setwd("C:/Users/jazzj/Downloads/DQ/dataset/MSOA")
  msoa1 <- read.csv("Local_Authority_District_to_Region_(December_2016)_Lookup_in_England.csv")
  msoa2<-read.csv("Local_Authority_District_to_Region_(December_2017)_Lookup_in_England.csv")
  msoa3<-read.csv("Local_Authority_District_to_Region_(December_2018)_Lookup_in_England.csv")
  msoa4<-read.csv("Local_Authority_District_to_Region__December_2019__Lookup_in_England.csv")
  
  return(list(lsoa, msoa1,msoa2,msoa3,msoa4,imd))
}



#Function to Merge MSOA Datasets from 2016 to 2019

merge_msoa <- function (msoa1,msoa2,msoa3,msoa4) 
{
  df1<-as.data.frame(msoa1)
  df1$year <- c(2016)
  df2<-as.data.frame(msoa2)
  df2$year <- c(2017)
  df3<-as.data.frame(msoa3)
  df3$year <- c(2018)
  df4<-as.data.frame(msoa4)
  df4$year <- c(2019)
  
  names(df1)[1] <- 'location_id'
  names(df1)[2] <- 'location_name'
  names(df1)[3] <- 'region_id'
  names(df1)[4] <- 'region_name'
  names(df1)[5] <- 'fid'
  names(df1)[6] <- 'year'
  
  names(df2)[1] <- 'location_id'
  names(df2)[2] <- 'location_name'
  names(df2)[3] <- 'region_id'
  names(df2)[4] <- 'region_name'
  names(df2)[5] <- 'fid'
  names(df2)[6] <- 'year'
  
  names(df3)[1] <- 'location_id'
  names(df3)[2] <- 'location_name'
  names(df3)[3] <- 'region_id'
  names(df3)[4] <- 'region_name'
  names(df3)[5] <- 'fid'
  names(df3)[6] <- 'year'
  
  names(df4)[1] <- 'location_id'
  names(df4)[2] <- 'location_name'
  names(df4)[3] <- 'region_id'
  names(df4)[4] <- 'region_name'
  names(df4)[5] <- 'fid'
  names(df4)[6] <- 'year'
  
  df<- rbind(df1,df2)
  df5<- rbind(df3,df4)
  dff<- rbind(df,df5)
  
  return (dff)
  
}

 
#Function to Merge All Datasets

merge_dataset <-function(acc_data,veh_data,msoa_data,lsoa_data,imd_data)
  
{
  
#Filtering out 2020 Data
  df1 <- as.data.frame(acc_data)
  accident_data <- subset(df1, df1$accident_year  >= '2016' &  df1$accident_year  <= '2019')
  
  accident_data
  df2 <- as.data.frame(veh_data)
  vehicle_data <- subset(df2, df2$accident_year  >= '2016' &  df2$accident_year  <= '2019')
  df5 <- as.data.frame(msoa_data)
  df4 <- as.data.frame(lsoa_data)
  df6 <- as.data.frame(imd_data)

#Merging Accident & Vehicle  
  mergeddata = merge(df1, df2, by.x=c('accident_reference','accident_year'), by.y=c('accident_reference','accident_year'))
  
#Merging Accident & Vehicle with LSOA Mapping
  merge2=merge(mergeddata, df4, by.x=c('lsoa_of_accident_location'), by.y=c('LSOA11CD'))
  nrow(merge2)
  imd_dataset <- subset(df6,select =c(lsoa11cd,lsoa11nm,IMD_Decile,OutScore,OutRank,OutDec,TotPop))
  nrow(imd_dataset)
  
#Merging IMD to Accident, Vehicle & LSOA Mapping
  merge3=merge(merge2,df6, by.x=c('lsoa_of_accident_location'), by.y=c('lsoa11cd'))
  merge3$date <- dmy(merge3$date)
  nrow(merge3)
  merge4 <- merge(merge3, mm, by.x=c('LAD19CD','accident_year'), by.y=c('LAD16CD','Year'))
  nrow(merge4)
  return(merge4)
  
}



#main
#Installing Required Packages

install.packages(c("httr", "jsonlite"))
install.packages("rjson")
library(httr)
library(jsonlite)
library("rjson")
library(dplyr) 
library(tidyverse)
library(readxl) 
library(tidyr)
library(stringr)
library(lubridate)


accident = accidentdata_api()
vehicle = vehicledata_api()
vehicle
data_list=data_fetch()
lsoa = data.frame(data_list[1])
msoa1 = data.frame(data_list[2])
msoa2 = data.frame(data_list[3])
msoa3 = data.frame(data_list[4])
msoa4 = data.frame(data_list[5])
imd = data.frame(data_list[6])
mm = data.frame(data_list[7])
mm
merged_msoa <- merge_msoa(msoa1,msoa2,msoa3,msoa4)
final_df <- merge_dataset(accident,vehicle,mm,lsoa,imd)
nrow(final_df)

#Extracting Necessary Columns
dataset <- subset(final_df,select =c(accident_year, accident_reference, date, day_of_week,time, lsoa_of_accident_location, LSOA11NM, LAD19CD, LAD19NM,
                                     latitude , longitude,RGN16CD, RGN16NM, accident_severity, number_of_casualties, road_type,  junction_detail, junction_control, light_conditions,
                                     weather_conditions, road_surface_conditions, special_conditions_at_site, urban_or_rural_area, vehicle_reference, vehicle_type,
                                     junction_location, sex_of_driver, age_of_driver, age_band_of_driver,  driver_imd_decile, OutScore, OutRank, OutDec, TotPop))


#Data Cleaning
#Replacing Missing Data with 999
data1 <- dataset %>% replace(.==-1, 999) # replace -1 with 999  
library(dplyr)
data2 <- data1 %>% replace(.=="NULL", 999)
data3 <- subset(data21,age_of_driver >14) # replace -1 with 999  

#Deleting Missing Values
nrow(data3)
data4 <- subset(data3,select =c( accident_severity !=999 & road_type  !=999 & junction_detail !=999 & junction_control !=999 & light_conditions !=999 &
                                   weather_conditions  !=999 & road_surface_conditions  !=999 & vehicle_reference  !=999 & vehicle_type  !=999 &
                                   junction_location  !=999 & sex_of_driver  !=999 & age_of_driver !=999 & age_band_of_driver  !=999 &  driver_imd_decile  !=999 &
                                   OutScore  !=999 & OutRank  !=999 & OutDec  !=999 & TotPop  !=999 ))


data5 <-subset(data4,junction_control <5)
data6 <-subset(data5,junction_detail !=99)
data7 <-subset(data6,junction_detail !=0)
nrow(data7)


#Writing Merged and Cleaned Data to CSV File for Upload to HDFS

write.csv(data7,"C:/Users/jazzj/Downloads/DQ/final_data.csv", row.names = FALSE)





