#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# In[2]:


loans_repay_schema = 'loan_id string, total_principal_received float, total_interest_received float, total_late_fee_received float, total_payment_received float, last_payment_amount float, last_payment_date string, next_payment_date string'


# In[3]:


loans_repay_raw_df = spark.read .format("csv") .option("header",True) .schema(loans_repay_schema) .load("/user/itv011631/lendingclub/raw/loan_repayments_df")


# In[4]:


from pyspark.sql.functions import current_timestamp
loans_repay_df_ingestd = loans_repay_raw_df.withColumn("ingest_date",current_timestamp())


# In[5]:


loans_repay_df_ingestd.createOrReplaceTempView("loan_replacement")


# In[6]:


columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]


# In[7]:


loans_repay_filtered_df = loans_repay_df_ingestd.na.drop(subset = columns_to_check )


# In[8]:


loans_repay_filtered_df.count()


# In[9]:


loans_repay_df_ingestd.count()


# In[10]:


loans_repay_filtered_df.createOrReplaceTempView("loan_repay")


# In[12]:


spark.sql("Select * from loan_repay where total_principal_received != 0.0 and total_payment_received = 0.0 ")


# In[18]:


from pyspark.sql.functions import when,col
loans_repay_fixed = loans_repay_filtered_df.withColumn("total_payment_received",
                                   when(
                                   (col("total_payment_received") == 0.0) & (col("total_principal_received") != 0.0),
                                   col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
                                   ).otherwise(col("total_payment_received"))
                                  )


# In[20]:


loans_payments_fixed2_df = loans_repay_fixed.filter("total_payment_received != 0.0")


# In[21]:


loans_payments_fixed2_df.filter("last_payment_date == 0.0").count()


# In[23]:


loans_payments_fixed2_df.filter("last_payment_date is null").count()


# In[22]:


loans_payments_fixed2_df.filter("next_payment_date == 0.0").count()


# In[24]:


loans_payments_fixed2_df.filter("next_payment_date is null").count()


# In[28]:


loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn("last_payment_date",
                                    when(
                                        (col("last_payment_date") == 0.0)
                                        ,None).otherwise(col("last_payment_date"))
                                   )


# In[29]:


loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn("next_payment_date",
                                    when(
                                        (col("next_payment_date") == 0.0)
                                        ,None).otherwise(col("next_payment_date"))
                                   )


# In[32]:


loans_payments_ndate_fixed_df.write .format("parquet") .mode("overwrite") .option("path", "/user/itv011631/lendingclub/cleaned/loans_repayments_parquet") .save()


# In[33]:


loans_payments_ndate_fixed_df.write .format("csv") .mode("overwrite") .option("path", "/user/itv011631/lendingclub/cleaned/loans_repayments_csv") .save()

