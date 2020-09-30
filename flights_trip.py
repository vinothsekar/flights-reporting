from pyspark.context import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
sqlContext = HiveContext(sc)

flights_data = sqlContext.sql("select * from refined_airlines.flights")
non_cancelled_flights = flights_data.filter(flights_data.cancelled == 0)

non_cancelled_longer_flights = non_cancelled_flights.filter(non_cancelled_flights.distance >= 1000)

grouped_data_count = non_cancelled_longer_flights.groupBy("flight_number").count()

grouped_distance_sum = non_cancelled_longer_flights.groupBy("flight_number").sum("distance")

df1 = grouped_data_count.alias('df1')
df2 = grouped_distance_sum.alias('df2')

trip_report = df1.join(df2,df1.flight_number == df2.flight_number,how='inner').select('df1.flight_number',col('df1.count').alias("total_trips"),col('df2.sum(distance)').alias("total_distance"))

trip_report.createOrReplaceTempView("trip_report_temp")

sqlContext.sql("insert into table refined_airlines.trip_reports select flight_number,total_trips,total_distance from trip_report_temp")