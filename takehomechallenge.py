


# Steps
#1: Add Kaggle creds

#3: clean the data
#4: take care of default values
#5: aggregate the data

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#1: Add Kaggle creds -- handled in dockerfile
# replace the username and creds

# Create SparkSession
spark = SparkSession.builder.appName("takehomechallenge").getOrCreate()

#2: Read data
inital_data = spark.read.csv("/root/pokemon/input/Pokemon.csv", header=True)

#3: clean data and fetch only required columns
cleansed_data = inital_data.withColumnRenamed("Sp. Atk", "Special_Attack")
filter_columns = ["Name","Type 1","Type 2","Total","HP","Special_Attack","Legendary"]
cleansed_data=cleansed_data.select(filter_columns)
#4: rename required columns : renaming Sp. ATk for dev friendly useage

#cleansed_data = inital_data.withColumnRenamed("Sp. Atk", "Special_Attack")
cleansed_data=cleansed_data.na.drop(subset=["HP"])
cleansed_data=cleansed_data.withColumn("HP", col("HP").cast("integer"))



# select * from pokemon where legendary=False order by total desc  limit 1
top_5_strongest_pokemon = cleansed_data.filter(col("Legendary") == False).orderBy(col("Total").desc()).limit(5)
print(f'top_5_strongest_pokemon')
top_5_strongest_pokemon.write.csv("/root/pokemon/output/top_5_strongest_pokemon", header=True,mode="overwrite")


#2: Which Pokemon type has the highest average HP?
#group by on type and fetch the limit 1

highest_avg_hp_type1 = cleansed_data.groupBy("Type 1").avg("HP").orderBy(col("avg(HP)").desc()).first()
print("Pokemon type with the highest average HP:", highest_avg_hp_type1)

highest_avg_Hp_type1=spark.createDataFrame([highest_avg_hp_type1])
highest_avg_Hp_type1.coalesce(1).write.csv("/root/pokemon/output/highest_avg_hp",header=True,mode="overwrite")



highest_avg_hp_type2 = cleansed_data.groupBy("Type 2").avg("HP").orderBy(col("avg(HP)").desc()).first()
print("Pokemon type with the highest average HP:", highest_avg_hp_type2)

highest_avg_Hp_type2=spark.createDataFrame([highest_avg_hp_type2])
highest_avg_Hp_type2.coalesce(1).write.csv("/root/pokemon/output/highest_avg_hp",header=True,mode="overwrite")



# select type1,type2,count(*),avg(HP) from pokemon
# group by type1,type 2 order by avg(HP) desc  limit 1

highest_avg_hp_combined = cleansed_data.groupBy(["Type 1","Type 2"]).avg("HP").orderBy(col("avg(HP)").desc()).first()
print("Pokemon type with the highest average HP:", highest_avg_hp_combined)

highest_avg_hp_combined=spark.createDataFrame([highest_avg_hp_combined])
highest_avg_hp_combined.coalesce(1).write.csv("/root/pokemon/output/highest_avg_hp_combined",header=True,mode="overwrite")

#3.	Which is the most common special Attack?
# select type1,type2,count(*) from pokemon
# group by Special_Attack order by 3 desc  limit 1
most_common_special_attack = cleansed_data.groupBy("Special_Attack").count().orderBy(col("count").desc()).first()
print("Most common special Attack:", most_common_special_attack)


most_common_special_attack=spark.createDataFrame([most_common_special_attack])
most_common_special_attack.coalesce(1).write.csv("/root/pokemon/output/most_common_special_attack",header=True,mode="overwrite")

## spark.stop()

