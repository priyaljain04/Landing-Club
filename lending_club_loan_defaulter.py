#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# In[2]:


loan_defaulters_schema = "member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float,inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"


# In[3]:


loans_defaulter_raw_df = spark.read .format("csv") .option("header",True) .schema(loan_defaulters_schema) .load("/user/itv011631/lendingclub/raw/loan_defaulter_df")


# In[ ]:





# In[4]:


loans_defaulter_raw_df


# In[5]:


from pyspark.sql.functions import count
loans_defaulter_raw_df.groupby("delinq_2yrs").agg(count("*").alias("total")).orderBy("total",ascending = False)


# In[6]:


from pyspark.sql.functions import col
loans_defaulter_raw_df = loans_defaulter_raw_df.withColumn("delinq_2yrs",col("delinq_2yrs").cast("integer")).fillna(0,subset = ["delinq_2yrs"])


# In[7]:


loans_defaulter_raw_df.createOrReplaceTempView("loan_defaulters")


# In[8]:


loans_def_delinq_df = spark.sql("select member_id,delinq_2yrs, delinq_amnt, int(mths_since_last_delinq) from loan_defaulters where delinq_2yrs > 0 or mths_since_last_delinq > 0")


# In[9]:


loans_def_delinq_df.count()


# In[10]:


loans_def_records_enq_df = spark.sql("select member_id from loan_defaulters where pub_rec > 0.0 or pub_rec_bankruptcies > 0.0 or inq_last_6mths > 0.0")


# In[11]:


loans_def_records_enq_df.count()


# In[12]:


loans_def_delinq_df.write .option("header", True) .format("csv") .mode("overwrite") .option("path", "/user/itv011631/lendingclub/cleaned/loans_defaulters_deling_csv") .save()


# In[13]:


loans_def_delinq_df.write .format("parquet") .mode("overwrite") .option("path", "/user/itv011631/lendingclub/cleaned/loans_defaulters_deling_parquet") .save()


# In[14]:


loans_def_records_enq_df.write .option("header", True) .format("csv") .mode("overwrite") .option("path", "/user/itv011631/lendingclub/cleaned/loans_defaulters_records_enq_csv") .save()


# In[15]:


loans_def_records_enq_df.write .option("header", True) .format("parquet") .mode("overwrite") .option("path", "/user/itv011631/lendingclub/cleaned/loans_defaulters_records_enq_parquet") .save()


# In[16]:


loans_def_records_details_enq_df = spark.sql("select member_id,pub_rec,pub_rec_bankruptcies,inq_last_6mths from loan_defaulters where pub_rec > 0.0 or pub_rec_bankruptcies > 0.0 or inq_last_6mths > 0.0")


# In[17]:


loans_def_records_details_enq_df = loans_defaulter_raw_df.withColumn("pub_rec",col("pub_rec").cast("integer")).fillna(0,subset = ["pub_rec"])


# In[18]:


loans_def_records_details_enq_df = loans_def_records_details_enq_df.withColumn("pub_rec_bankruptcies",col("pub_rec_bankruptcies").cast("integer")).fillna(0,subset = ["pub_rec_bankruptcies"])
loans_def_records_details_enq_df = loans_def_records_details_enq_df.withColumn("inq_last_6mths",col("inq_last_6mths").cast("integer")).fillna(0,subset = ["inq_last_6mths"])


# In[19]:


loans_def_records_details_enq_df.createOrReplaceTempView("loan_defaulters")


# In[21]:


loans_def_records_new_details_enq_df = spark.sql("select member_id,pub_rec,pub_rec_bankruptcies,inq_last_6mths from loan_defaulters ")


# In[22]:


loans_def_records_new_details_enq_df.write .option("header", True) .format("parquet") .mode("overwrite") .option("path", "/user/itv011631/lendingclub/cleaned/loans_def_records_new_details_enq_df") .save()


# In[23]:


loans_def_records_new_details_enq_df.write .option("header", True) .format("csv") .mode("overwrite") .option("path", "/user/itv011631/lendingclub/cleaned/loans_def_records_new_details_enq_df_csv") .save()


# In[ ]:




