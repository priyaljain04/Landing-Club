#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession. builder. config('spark.ui.port', '0'). config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). enableHiveSupport(). master('yarn'). getOrCreate()


# In[2]:


loan_df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("/user/itv011631/lendingclub/raw/loan_df")


# In[3]:


loans_schema = 'loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string, interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string'


# In[4]:


loan_df = spark.read.format("csv").option("header",True).option("inferSchema",True).schema(loans_schema).load("/user/itv011631/lendingclub/raw/loan_df")


# In[5]:


from pyspark.sql.functions import current_timestamp
loans_df_ingestd = loan_df.withColumn("ingest_date", current_timestamp())


# In[6]:


columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]


# In[7]:


loans_df_ingestd = loans_df_ingestd.na.drop(subset = columns_to_check)


# In[8]:


loans_df_ingestd


# In[9]:


from pyspark.sql.functions import regexp_replace,col
loans_df_ingestd_new = loans_df_ingestd.withColumn("loan_term_months",(regexp_replace(col("loan_term_months"),"\D","").cast('int')/12).cast('int'))


# In[10]:


loans_df_ingestd_new.withColumnRenamed("loan_term_months","loan_term_years")


# In[11]:


loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]


# In[12]:


from pyspark.sql.functions import when
loan_purpose_modified = loans_df_ingestd_new.withColumn("loan_purpose",when(col("loan_purpose").isin(loan_purpose_lookup),col("loan_purpose")).otherwise("Other"))


# In[13]:


from pyspark.sql.functions import count
loan_purpose_modified.groupBy("loan_purpose").agg(count("*").alias("total")).orderBy(col("total").desc())


# In[15]:


loan_purpose_modified.write.format("parquet").mode("overwrite").option("path","/user/itv011631/lendingclub/cleaned/loan_purpose_modified").save()
loan_purpose_modified.write.format("csv").option("header",True).mode("overwrite").option("path","/user/itv011631/lendingclub/cleaned/loan_purpose_modified_csv").save()

