# Databricks notebook source
import pyspark.sql.functions as f
import sys
print(sys.version)

newschema="lb_income_band long ,ib_lb int , ib_up int"
incomeBandDf=spark.read.schema(newschema).option("sep","|").format('csv').load('/FileStore/tables/retailer/data/income_band.dat')
incomeBandDf.printSchema()

# COMMAND ----------

incomeBandDf.show()

# COMMAND ----------

from pyspark.sql.functions import col


incomeBandDfGroup=incomeBandDf\
.withColumn("firstIncomegroup",f.col("ib_up")<=60000)\
.withColumn("secondgroup",(f.col("ib_up")>=60000) & (f.col("ib_up")< 120001))

incomeBandDfGroup.show()

# COMMAND ----------

customerDf=spark.read.option("sep","|").option('header',True).option('inferSchema',True).format('csv').load('/FileStore/tables/retailer/customer.dat')
customerDf.printSchema()

# COMMAND ----------

# customerDfWithValidBirthInfo=customerDf.filter(customerDf.c_birth_month> 5)
# customerDfWithValidBirthInfo=customerDf.filter((col("c_birth_month") >5)  & (col("c_birth_month") < 10) )
# display(customerDfWithValidBirthInfo)
customerDfWithValidBirthInfo2=customerDf.where("c_birth_month <10")   
print(type(customerDfWithValidBirthInfo2))
customerDfWithValidBirthInfo3=customerDfWithValidBirthInfo2.filter(col("c_birth_month") >5)

# COMMAND ----------



# COMMAND ----------

