#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession. builder. config('spark.ui.port', '0'). config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). enableHiveSupport(). master('yarn'). getOrCreate()


# In[2]:


raw_df = spark.read.format("csv").option("InferSchema","True").option("header",True).load("/public/trendytech/datasets/accepted_2007_to_2018Q4.csv")


# In[3]:


raw_df.createOrReplaceTempView("lending_club_data")


# In[4]:


spark.sql("Select * from lending_club_data")


# In[5]:


from pyspark.sql.functions import sha2,concat_ws
#new_df = raw_df.withColumn("name_sh2",sha2(concat_ws("||",*["emp_title","emp_length","home_ownership","annual_inc","zip_code","addr_state","grade","sub_grade","verification_status"]),256))
new_df = raw_df.withColumn("name_sh2", sha2(concat_ws("||", *["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]), 256))


# In[6]:


new_df.createOrReplaceTempView("newtable")


# In[7]:


spark.sql("Select count(1) from newtable")


# In[8]:


spark.sql("Select count(distinct name_sh2) from newtable").show()


# In[9]:


spark.sql("""Select name_sh2,count(*) as total_count from newtable 
            group by name_sh2 having total_count>1
            order by total_count desc""")


# In[10]:


spark.sql("""select name_sh2 as member_id,emp_title,emp_length,home_ownership,annual_inc,addr_state,zip_code,'USA' as country,grade,sub_grade,
verification_status,tot_hi_cred_lim,application_type,annual_inc_joint,verification_status_joint from newtable""").repartition(1).write.format("csv").option("header",True)\
.mode("overwrite").option("path","/user/itv011631/lendingclub/raw/customer_df").save()


# In[11]:


spark.sql("""select id as loan_id, name_sh2 as member_id,loan_amnt,funded_amnt,term,int_rate,installment,issue_d,loan_status,purpose,
           title from newtable""").repartition(1).write.format("csv").option("header",True) \
.mode("overwrite").option("path","/user/itv011631/lendingclub/raw/loan_df").save()


# In[12]:


spark.sql("""select id as loan_id,total_rec_prncp,total_rec_int,total_rec_late_fee,total_pymnt,last_pymnt_amnt,last_pymnt_d,next_pymnt_d from newtable""").repartition(1).write.format("csv").option("header",True).mode("overwrite").option("path","/user/itv011631/lendingclub/raw/loan_repayments_df").save()


# In[13]:


spark.sql("""select name_sh2 as member_id,delinq_2yrs,delinq_amnt,pub_rec,pub_rec_bankruptcies,inq_last_6mths,total_rec_late_fee,mths_since_last_delinq,mths_since_last_record from newtable""").repartition(1).write.format("csv").option("header",True).mode("overwrite").option("path","/user/itv011631/lendingclub/raw/loan_defaulter_df").save()


# In[14]:


customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string'


# In[15]:


customer = spark.read.format("csv").option("header",True).schema(customer_schema).load("/user/itv011631/lendingclub/raw/customer_df")


# In[16]:


customer.printSchema()


# In[17]:


customer = customer.withColumnRenamed("annual_inc","annual_income")         .withColumnRenamed("addr_state", "address_state")         .withColumnRenamed("zip_code", "address_zipcode")         .withColumnRenamed("country", "address_country")         .withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit")         .withColumnRenamed("annual_inc_joint", "join_annual_income")


# In[18]:


from pyspark.sql.functions import current_timestamp
customer_df = customer.withColumn("ingest",current_timestamp())


# In[19]:


customer_df.count()


# In[20]:


customer_df.distinct().count()


# In[21]:


customer_df_dist = customer_df.distinct() 


# In[22]:


customer_df_dist.createOrReplaceTempView("customer_data")


# In[23]:


customer_df_dist.printSchema()


# In[24]:


customer_data =  spark.sql("Select * from customer_data where annual_income is not null")


# In[25]:


customer_data.createOrReplaceTempView("customer_new")


# In[26]:


spark.sql("Select distinct(emp_length)  from customer_new")


# In[27]:


from pyspark.sql.functions import regexp_replace,col


# In[28]:


customer_data_cleaned = customer_data.withColumn("emp_length",regexp_replace(col("emp_length"),"\D",""))


# In[29]:


customer_emp_length_casted = customer_data_cleaned.withColumn("emp_length",customer_data_cleaned.emp_length.cast('int'))


# In[30]:


customer_emp_length_casted.filter("emp_length is null").count()


# In[31]:


customer_emp_length_casted.createOrReplaceTempView("customer_emp_length_casted")


# In[35]:


avg_emp_length = spark.sql("Select floor(avg(emp_length)) as avg_emp_length from customer_emp_length_casted").collect()


# In[38]:


avg_length = avg_emp_length[0][0]


# In[39]:


customer_emp_length_casted.na.fill(avg_length,subset = ['emp_length'])


# In[41]:


from pyspark.sql.functions import when,col,length
customer_state_cleaned = customer_emp_length_casted.withColumn("address_state",when(length(col("address_state")) > 2, "NA").otherwise(col("address_state")))


# In[42]:


customer_state_cleaned.select("address_state").show()


# In[43]:


customer_state_cleaned.write.format("parquet").mode("overwrite").option("path","/user/itv011631/lendingclub/cleaned/customer").save()


# In[44]:


customer_state_cleaned.write.format("parquet").mode("overwrite").option("path","/user/itv011631/lendingclub/cleaned/customer_csv").save()


# In[ ]:




