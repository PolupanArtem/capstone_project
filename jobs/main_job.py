import sys
sys.path.extend(['/Users/polupanartem/Desktop/grid_capstone_project/jobs/', '/Users/polupanartem/Desktop/grid_capstone_project'])
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    when,
    udf,
    sum,
    last,
    get_json_object,
    lit,
    length,
    regexp_replace,
    monotonically_increasing_id
)

from pyspark.sql.types import (
    MapType,
    StringType,
    StructType,
    IntegerType,
    BooleanType,
    DoubleType,
    TimestampType,
)
from enum import Enum
from save_to_parquet import save_data_to_parquet

from channels_engagement import (
  channels_engagement_sql,
  channels_engagement_spark_df
)
from top_campaigns import (
  top_campaigns_sql,
  top_campaigns_spark_df
)

class Event_Type(str, Enum):
    OPEN = 'app_open'
    SEARCH = 'search_product'
    PRODUCT_DETAILS = 'view_product_details'
    PURCHASE = 'purchase'
    CLOSE = 'app_close'

class JOIN_TYPE(str, Enum):
  LEFT = 'left'

def main():
    spark = (
        SparkSession
        .builder
        .appName('mobile_app_click_stream')
        .getOrCreate()
    )
    task_1_path = './data/test/transform_result'
    # task_1_path = './output/task1.1/target_schema'

    clicks_data = load_clickstream_data(spark)
    user_purchases_data = load_user_purchases_data(spark)

    transformed_data = transform_data(clicks_data, user_purchases_data)
    transformed_data.show()
    transformed_data.createOrReplaceTempView('target')

    save_data_to_parquet(transformed_data, task_1_path)

    ## UDF
    # transformed_data_udf = transform_data_with_udf(clicks_data, user_purchases_data)
    # transformed_data_udf.show()
    # transformed_data_udf.createOrReplaceTempView('target')
    # save_data_to_parquet(transformed_data_udf, task_1_path)
    ## UDF

    top_campaigns_spark_df(transformed_data)
    top_campaigns_sql(spark)
  
    channels_engagement_spark_df(transformed_data)
    channels_engagement_sql(spark)

    spark.stop()


clickstream_schema = (
    StructType()
    .add('userId', StringType(), False)
    .add('eventId', StringType(), False)
    .add('eventType', StringType(), False)
    .add('eventTime', TimestampType(), False)
    .add('attributes', StringType(), True)
    # .add('attributes', MapType(StringType(), StringType()), True)
)

purchase_schema = (
    StructType()
    .add('purchaseId', StringType(), False)
    .add('purchaseTime', TimestampType(), False)
    .add('billingCost', DoubleType(), True)
    .add('isConfirmed', BooleanType(), True)
)


def load_clickstream_data(spark):
    return (
      spark.read.format('csv')
        .options(header='True')
        .schema(clickstream_schema)
        .load('./data/mobile_app_clickstream/mobile_app_clickstream_1.csv.gz')
        # .load('./data/mobile_app_clickstream/*.csv.gz')
    )



def load_user_purchases_data(spark):
    return (
      spark.read.format('csv')
        .options(header='True')
        .schema(purchase_schema)
        .load('./data/user_purchases/user_purchases_1.csv.gz')
        # .load('./data/user_purchases/*.csv.gz')
    )


# Task 1.1  Implement it by utilizing default Spark SQL capabilities.
def transform_data(clickstream_data, user_purchases_data):
    window1 = Window.partitionBy('userId').orderBy('eventTime')
    window2 = Window.orderBy('sessionId')
    appOpenFlagDefaultVal = 0

    clickstream_data = (
      clickstream_data
        .withColumn('appOpenFlag',
          when(
            (col('eventType') == Event_Type.OPEN),
            monotonically_increasing_id()
          ).otherwise(appOpenFlagDefaultVal)
        )
        .withColumn('sessionId', sum(col('appOpenFlag')).over(window1))
        .withColumn('attrs',
          when(
            col('eventType') == Event_Type.PURCHASE,
            clickstream_data['attributes'].substr(lit(2), length('attributes') - lit(2))
          ).otherwise(col('attributes'))
        )
        .withColumn('attr',
          when(
            col('eventType') == Event_Type.PURCHASE,
            regexp_replace(col('attrs'), '""', "'")
          ).otherwise(col('attrs'))
        )
        .withColumn('campaign_id',
          when(
            get_json_object('attr', '$.campaign_id').isNotNull(),
            get_json_object('attr', '$.campaign_id')
          ).otherwise(None)
        )
        .withColumn('channel_id',
          when(
            get_json_object('attr', '$.channel_id').isNotNull(),
            get_json_object('attr', '$.channel_id')
          ).otherwise(None)
        )
        .withColumn('purchase_id',
          when(
            get_json_object('attr', '$.purchase_id').isNotNull(),
            get_json_object('attr', '$.purchase_id')
          ).otherwise(None)
        )
        .withColumn('campaignId', last(col('campaign_id'), ignorenulls=True).over(window2.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn('channelId', last(col('channel_id'), ignorenulls=True).over(window2.rowsBetween(Window.unboundedPreceding, 0)))
    )

    target_df = clickstream_data.join(
      user_purchases_data,
      clickstream_data['purchase_id'] == user_purchases_data['purchaseId'],
      JOIN_TYPE.LEFT
    )
    return target_df.select(
      col('purchaseId'),
      col('purchaseTime'),
      col('billingCost'),
      col('isConfirmed'),
      col('sessionId'),
      col('campaignId'),
      col('channelId')
    )

# Task 1.2 Implement it by using a custom UDF.
count_sessions = 0

def increase_session_count(event_type):
    global count_sessions
    if event_type == Event_Type.OPEN:
        session_id = count_sessions
        count_sessions += 1
        return session_id
    else:
        return None


def clear_attributes(event_type, attributes):
    attr = attributes
    if event_type == Event_Type.PURCHASE:
        attr = attributes[1 : len(attributes) - 1].replace('""', "'")
    return attr


app_open_flag_udf = udf(increase_session_count, IntegerType())
attributes_udf = udf(clear_attributes, StringType())


def transform_data_with_udf(clickstream_data, purchase_data):
    window1 = Window.partitionBy('userId').orderBy('eventTime')
    window2 = Window.orderBy('sessionId')

    clickstream_data = (clickstream_data
        .withColumn('appOpenFlag', app_open_flag_udf(clickstream_data['eventType']))
        .withColumn('sessionId', sum(col('appOpenFlag')).over(window1))
        .withColumn('attr', attributes_udf(clickstream_data['eventType'], clickstream_data['attributes']))
        .withColumn('campaign_id',
          when(
            get_json_object('attr', '$.campaign_id').isNotNull(),
            get_json_object('attr', '$.campaign_id')
          ).otherwise(None)
        )
        .withColumn('channel_id',
          when(
            get_json_object('attr', '$.channel_id').isNotNull(),
            get_json_object('attr', '$.channel_id')
          ).otherwise(None)
        )
        .withColumn('purchase_id',
          when(
            get_json_object('attr', '$.purchase_id').isNotNull(),
            get_json_object('attr', '$.purchase_id')
          ).otherwise(None)
        )
        .withColumn('campaignId',last(col('campaign_id'), ignorenulls=True)
          .over(window2.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn('channelId',last(col('channel_id'), ignorenulls=True)
          .over(window2.rowsBetween(Window.unboundedPreceding, 0)))
      )

    target_df = clickstream_data.join(
      purchase_data,
      clickstream_data['purchase_id'] == purchase_data['purchaseId'], 
      JOIN_TYPE.LEFT
    )

    return target_df.select(
      col('purchaseId'),
      col('purchaseTime'),
      col('billingCost'),
      col('isConfirmed'),
      col('sessionId'),
      col('campaignId'),
      col('channelId')
    )

if __name__ == '__main__':
    main()
