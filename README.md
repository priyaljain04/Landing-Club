# Lending-Club

The Lending Club project involves calculating a loan score based on three major factors: Loan Payment History, Customer’s Financial Health, and Loan Defaulters History. The project includes data cleaning, transformation, and storage processes using Apache PySpark, and the final cleaned data is used to create permanent tables for downstream analytics.

# Key Steps and Components
## 1. Data Creation and Cleaning
   1. Created 4 different Dataset - Customer, Loan Deafaulters , Loan and Loan repayments
   2. Loan Payment History: Data includes historical payments, interest received, and late fees.
   3. Customer’s Financial Health: Data includes customer employment details, income, home ownership, etc. 
   4. Loan Defaulters History: Data includes delinquencies, public records, bankruptcies, and enquiries.

Performed initial cleaning by removing unnecessary columns , renaming columns,created new columns to summarize loan and repayment data and addressing missing values with appropriate strategies (e.g., filling with mean/median values)



## 2. Created Processed Dataframes
![image](https://github.com/priyaljain04/Landing-Club/assets/44484014/0549717c-913e-4e59-bbfe-0943068ab0d3)


## 3. Created permanent tables for multiple teams to access the cleaned data.

## 4. Created a view on the cleaned data that refreshes every 24 hours for up-to-date analysis.

## 5.Loan Score Calculation
   1.Identified the Bad Data and Final Cleaning - Identified the repeated customer id and removed the duplicate one's 

   2.Calculated Loan Score - 
   Built a comprehensive scoring system based on various factors:
      1. Payment History: Analyzed the borrowers' payment patterns, including last payment amount and date.
      2. Credit Score: Incorporated borrowers' credit scores to assess their financial health.
      3. Employment and Public Records: Evaluated the stability of employment and checked for any negative public records such as bankruptcies.

   3. Combining Scores for Final Evaluation:

      Created a final loan score by combining different metrics, such as payment history, credit score, and public records.
      Implemented a weighting system where payment history had the highest weight (50%), followed by financial health indicators (30%), and employment/public records (20%).
   


