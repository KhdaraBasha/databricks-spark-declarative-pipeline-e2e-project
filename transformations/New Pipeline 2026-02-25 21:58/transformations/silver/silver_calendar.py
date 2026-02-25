# Import necessary libraries
from pyspark import pipelines as dp
from pyspark.sql.functions import dayofmonth, month, year, dayofweek, weekofyear, quarter, last_day, dayofyear, when, date_format
from datetime import datetime, timedelta

# declare start_date and end_date variables
start_date = spark.conf.get("start_date")
end_date = spark.conf.get("end_date")

# Create a function to generate calendar table with important variables only
@dp.table(
    name="transportation.silver.calendar_view",
    comment="Calendar table with important variables only",
    table_properties={
        'quality': 'silver',
        'layer': 'silver',
        'delta.autoOptimize.optimizeWrite': "true",
        'delta.autoOptimize.autoCompact': "true",
        "delta.enableChangeDataFeed": "true"}
)
def generate_calendar():
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    dates = [start_dt + timedelta(days=x) for x in range((end_dt - start_dt).days + 1)]
    calendar = spark.createDataFrame([(d,) for d in dates], schema='date DATE')

    calendar = (
        calendar.withColumn('date', calendar['date'].cast('date'))
                .withColumn('datekey', date_format('date', 'yyyyMMdd'))
                .withColumn('day', dayofmonth('date'))
                .withColumn('month', month('date'))
                .withColumn('year', year('date'))
                .withColumn('day_of_week', dayofweek('date'))
                .withColumn('week', weekofyear('date'))
                .withColumn('quarter', quarter('date'))
                .withColumn('is_weekend', when((dayofweek('date') == 1) | (dayofweek('date') == 7), True).otherwise(False))
                .withColumn('is_month_start', when(dayofmonth('date') == 1, True).otherwise(False))
                .withColumn('is_month_end', when(dayofmonth('date') == dayofmonth(last_day('date')), True).otherwise(False))
                .withColumn('is_year_start', when(dayofyear('date') == 1, True).otherwise(False))
                .withColumn('is_year_end', when(dayofyear('date') == dayofyear(last_day('date')), True).otherwise(False))
    )
    return calendar