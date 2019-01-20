import snowflake.connector
"""this class will handle the rent elt from the datalake through stg to the DWH
it handles a snowflake db connection.
Ideally the connection details will be arguments from outside the class so we can control the environment that the
process will run on.
"""
class RentETL:

    def __init__(self, user, passw, account):
        self.con = snowflake.connector.connect(
        user=user,
        password=passw,
        account=account )



    def run_sql(self,sql_code):
        cur=self.con.cursor()
        cur.execute(sql_code)
        cur.close()

    def compare_load_results(self):
        """this function can validate that the load of the fact is done without losing data
        we can also include here other validations.
        I did not write this method. just wanted to show intention"""


query_truncate_stg="""truncate table stg.marketing.fact_rent;"""
query_load_stg="""
insert into stg.marketing.fact_rent
    (flat_id, living_rooms , phone_number,agency_name ,email ,agent_name ,flat_id,when_updated ,when_created ,city ,
    house_number ,district ,street ,latitude,longitude ,toilets,flat size,bathrooms ,sleeping_rooms ,number_of_rooms,
    parking_lot_price,has_flat_pictures,price )
(
    select flat_id, living_rooms , phone_number,agency_name ,email ,agent_name ,flat_id,when_updated ,when_created ,city ,
            house_number ,district ,street ,latitude,longitude ,toilets,flat size,bathrooms ,sleeping_rooms ,number_of_rooms,
            parking_lot_price,has_flat_pictures,price ,
            1 as is_active
    from    ods.rent.v_immobilienscout24 
    where   when_created>=(select max(when_created) from dwh.marketing.fact_rent ) --incremental load
);

"""
query_merge_stg_2_dwh="""
merge into dwh.marketing.fact_rent t1 using stg.marketing.fact_rent t2 on t1.flat_id = t2.flat_id 
when matched  then update set 
    living_rooms 	=	t2.living_rooms ,
    phone_number	=	t2.phone_number,
    agency_name 	=	t2.agency_name ,
    email 	=	t2.email ,
    agent_name 	=	t2.agent_name ,
    flat_id	=	t2.flat_id,
    when_updated 	=	t2.when_updated ,
    when_created 	=	t2.when_created ,
    city 	=	t2.city ,
    house_number 	=	t2.house_number ,
    district 	=	t2.district ,
    street 	=	t2.street ,
    latitude	=	t2.latitude,
    longitude 	=	t2.longitude ,
    toilets	=	t2.toilets,
    flat size	=	t2.flat size,
    bathrooms 	=	t2.bathrooms ,
    sleeping_rooms 	=	t2.sleeping_rooms ,
    number_of_rooms	=	t2.number_of_rooms,
    parking_lot_price	=	t2.parking_lot_price,
    has_flat_pictures	=	t2.has_flat_pictures,
    price 	=	t2.price  
    
 when not matched then insert
 (flat_id, living_rooms , phone_number,agency_name ,email ,agent_name ,flat_id,when_updated ,when_created ,city ,
house_number ,district ,street ,latitude,longitude ,toilets,flat size,bathrooms ,sleeping_rooms ,number_of_rooms,
parking_lot_price,has_flat_pictures,price )
values
(
t2.flat_id, t2.living_rooms ,t2.phone_number,t2.agency_name ,t2.email ,t2.agent_name ,t2.flat_id,t2.when_updated ,t2.when_created ,
t2.city ,t2.house_number ,t2.district ,t2.street ,t2.latitude,t2.longitude ,t2.toilets,t2.flat size,t2.bathrooms ,
t2.sleeping_rooms ,t2.number_of_rooms,t2.parking_lot_price,t2.has_flat_pictures,t2.price 
);
"""

query_update_non_active="update dwh.marketing.fact_rent set is_active=0 where flat_id not in " \
                        "(select flat_id from stg.marketing.fact_rent);"


# run the workflow in order
ETL=RentETL('ash1','Aash999d123','ad75875.us-east-2')
ETL.run_sql(query_truncate_stg)
ETL.run_sql(query_load_stg)
ETL.run_sql(query_merge_stg_2_dwh)
ETL.run_sql(query_update_non_active)
ETL.compare_load_results()


