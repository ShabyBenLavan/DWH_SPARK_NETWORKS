# DWH Coding exersize suggested solution
##### written by Shaby Ben-Lavan
##


Obviously, in order to create this ETL I had to make some assumptions.
Also - I could not cover all the corners that this kind of task holds.
So I took the privilege to focus on the fundamentals and best practices in such a task.



## 1. Loading the raw json file into snowflake datalake

  - I have assumed that the raw data file is generated in s3 bucket every hour.
  - Snowflake has a built in trigger (lambda function) called SNOWPIPE to load data into snowflake DB. The advantage of using a Snowflake datalake is that it allows our data manipulation process to be exclusively  in the database.(ELT)
  - here is the code to create the "snowpipe"
```sql
create schema ods.rent;
create table ods.rent.immobilienscout24_json_data
(raw_data variant, ts timestamp);

create pipe if not exists  ods.rent.immobilienscout24 auto_ingest=true as 
copy into  ods.rent.immobilienscout24_json_data
from
(
 select $1 ,TO_TIMESTAMP_NTZ (current_timestamp())
    from       @PUBLIC.SNOWFLAKE_DATA_LAKE/rent
)
file_format = 'JSON_FORMAT'
arn:aws:sqs:xs-east-1:0217120c285:sf-snowpipe-ADDI2DJEQQEcND6JPBXDQ-qa_6lisRAsDFiJE47hWSIg
```
 - We also need to define in AWS the bucket event :properties->Events and create new event -->ObjectCreate(ALL) -so now we have otomated the json load into the DB.
 




# 2. S2T

In order to review the fields in the json file I wrote this code:
```python
import json
f= open("d:/immobilienscout24_berlin_20190113.json") 
kk=[]
for lines in f:
    l=json.loads(lines)['data']
    for k in l.keys():
        if k not in kk:
            kk.append(k)
            
for i in kk:
    print (i)
```    

I have manually matched the fields that were listed in the task.
here is the s2t table


|                                                                            |                                                                                                                                                  |                                                       |                                                                            | 
|----------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|----------------------------------------------------------------------------| 
| source column                                                              | data type                                                                                                                                        | target column                                         | remarks                                                                    | 
| id                                    | varchar                                                                                                                                          | flat_id                                               |                                                                            | 
| realEstate_livingSpace                                                     | decimal                                                                                                                                          | flat_size                                             |                                                                            | 
| realEstate_numberOfRooms                                                   | decimal                                                                                                                                          | number_of_rooms,                                      |                                                                            | 
| realEstate_numberOfBedRooms                                                | decimal                                                                                                                                          | sleeping_rooms,                                       |                                                                            | 
| *******                                                                    | decimal                                                                                                                                          | living_rooms,                                         | does it exists in the source?                                              | 
| realEstate_numberOfBathRooms                                               | decimal                                                                                                                                          | bathrooms,                                            |                                                                            | 
| realEstate_guestToilet                                                     | decimal                                                                                                                                          | toilets,                                              |                                                                            | 
| realEstate_totalRent                                                       | decimal                                                                                                                                          | price,                                                |                                                                            | 
| *******                                                                    |                                                                                                                                                  | cold_and_warm,                                        | can be extracted from HTML TEXT - I couldn't find any other matching field | 
| *******                                                                    |                                                                                                                                                  | additional_expenses,                                  | can be summed from some cost fees e.g heating price                        | 
| realEstate_parkingSpacePrice                                               | decimal                                                                                                                                          | parking_lot_price                                     |                                                                            | 
| realEstate_address_geoHierarchy_city_name                                  | varchar                                                                                                                                          | city,                                                 |                                                                            | 
| realEstate_address_quarter                                                 | varchar                                                                                                                                          | district,                                             |                                                                            | 
| realEstate_titlePicture_id                                                 | varchar                                                                                                                                          | has_flats_pictures                                   |                                                                            | 
| realEstate_address_street                                                  | varchar                                                                                                                                          | street,                                               |                                                                            | 
| realEstate_address_houseNumber                                             | int                                                                                                                                              | house_number,                                         |                                                                            | 
| realEstate_address_wgs84Coordinate_longitude                               | decimal                                                                                                                                          | longitude,                                            |                                                                            | 
| realEstate_address_wgs84Coordinate_latitude                                | decimal                                                                                                                                          | latitude                                              |                                                                            | 
| contactDetails_company                                                     | varchar                                                                                                                                          | agency_name,                                          |                                                                            | 
| contactDetails_firstname+contactDetails_lastname                           | varchar                                                                                                                                          | agent_name,                                           |                                                                            | 
| contactDetails_email                                                       | varchar                                                                                                                                          | email,                                                |                                                                            | 
| contactDetails_cellPhoneNumber                                             | varchar                                                                                                                                          | phone_number_hashed   
| *******                                                                    | varchar                                                                                                                                          | is_active,|calculate by sql                                            |                                                                            | 
| publishDate                                                                | timestamp                                                                                                                                        | when_created,                                         |                                                                            | 
| modification                                                               | timestamp                                                                                                                                        | when_updated,                                         |                                                                            | 



## 3. Create a database view to parse the nessacery fields from the json table.
It contains only the nessassery fields for the ETL process.
It also takes for each flat_id only it's latest extracted record from the datalake - so we can be sure we are updating the most recent data 

```sql
create view
ods.rent.v_immobilienscout24
as
(select  src:id	as 	flat_id	,
md5(src:contactDetails_cellPhoneNumber)	as 	phone_number	,
src:contactDetails_company	as 	agency_name 	,
src:contactDetails_email	as 	email 	,
src:contactDetails_firstname||' '||src:contactDetails_lastname	as 	agent_name 	,
to_timestamp(src:modification,'YYYYMMDDHHMISSMI')	as 	when_updated 	,
to_timestamp(src:publishDate,'YYYYMMDDHHMISSMI')	as 	when_created 	,
src:realEstate_address_geoHierarchy_city_name	as 	city 	,
src:realEstate_address_houseNumber	as 	house_number 	,
src:realEstate_address_quarter	as 	district 	,
src:realEstate_address_street	as 	street 	,
src:realEstate_address_wgs84Coordinate_latitude	as 	latitude	,
src:realEstate_address_wgs84Coordinate_longitude	as 	longitude 	,
src:realEstate_guestToilet	as 	toilets	,
src:realEstate_livingSpace	as 	flat_size	,
src:realEstate_numberOfBathRooms	as 	bathrooms 	,
src:realEstate_numberOfBedRooms	as 	sleeping_rooms 	,
src:realEstate_numberOfRooms	as 	number_of_rooms	,
src:realEstate_parkingSpacePrice	as 	parking_lot_price	,
nvl(src:realEstate_titlePicture_id, '0' , '1')	as 	has_flat_pictures	,
src:realEstate_totalRent	as 	price 	
 
 FROM  ods.rent.immobilienscout24_json_data a,
	   (	
		select src:id as flat_id, max(ts) ts
		from ods.rent.immobilienscout24_json_data 
		group by src:id
	   )b
	   
where b.flat_id=a.flat_id	
and   b.ts=a.ts  --this will make each falt in the view - show only it's recent record
)
```


# ELT WITH Python & Snowflake
> Now that we have mapped our source fields to fact, there are 3 parts in our elt class

> 1. I assume that the dwh.marketing.fact_rent already exists with it's corresponding staging table.
> 2. Truncate the stg table
> 3. Incremental load of the recent data from the datalake to the staging table.
> 2. merging the new data from the stg table  into our fact table (upsert)
> 3. marking absent flat ads as inactive (update is_active field)
> 4. data verification process

I did not handle data validation in the scope of this task (missing values etc..) 
## please find the RentETL.py file for details


------------------------------

# Build a star, or snowflake (the one which you think is more appropriate) data model in the DB of your choice.

I would choose a star schema in order to model the data.
Even though this model is not normalized - it is easy to maintained.
With current storage costs even  large dimention table are worth keeping since the cost is low.
Also Tableau supports well a star schema model. it will use the dimentions in order to filter data effectivly.






