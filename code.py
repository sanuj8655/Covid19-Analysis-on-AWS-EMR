from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("covid").getOrCreate()

cowid = spark\
    .read\
    .format("csv")\
    .options(header = 'true', inferSchema = 'true')\
    .load("dataset/owid-covid-data.csv")

recovered_data = spark\
    .read\
    .format("csv")\
    .options(header = 'true', inferSchema = 'true')\
    .load("dataset/countries-aggregated.csv")

df1 = cowid.drop("reproduction_rate", "weekly_icu_admissions","weekly_icu_admissions_per_million","weekly_hosp_admissions","weekly_hosp_admissions_per_million","total_tests_per_thousand","new_tests_per_thousand","new_tests_smoothed","new_tests_smoothed_per_thousand","tests_per_case","tests_units","total_boosters","new_vaccinations_smoothed","total_vaccinations_per_hundred","people_vaccinated_per_hundred","people_fully_vaccinated_per_hundred","total_boosters_per_hundred","stringency_index","population_density","median_age","aged_65_older","aged_70_older","gdp_per_capita","extreme_poverty","cardiovasc_death_rate","cardiovasc_death_rate","female_smokers","male_smokers","handwashing_facilities","hospital_beds_per_thousand","human_development_index","excess_mortality_cumulative_absolute","excess_mortality_cumulative","excess_mortality","excess_mortality_cumulative_per_million","diabetes_prevalence")

df1.write.saveAsTable("q_cowid",mode="OVERWRITE")

df2 = spark.read.table("q_cowid")

df3=df2.na.drop(subset=["continent"])

df4 = df3.coalesce(1)

df5=df4.withColumnRenamed('date','Date_2')

cond = [df5.Date_2 == recovered_data.Date , df5.location == recovered_data.Country]

df6 = df5.join(recovered_data, cond,'inner')

df7 = df6.drop("Date","Country","Confirmed","Deaths")

df8 = df7.withColumn("Active",df7.total_cases-df7.Recovered)

df9 = df8.fillna(value=0)

df9.write.parquet("s3://covid-data1/cowid.parquet")

df9.show(truncate=False)

spark.stop()
