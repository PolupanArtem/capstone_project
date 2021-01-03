from save_to_parquet import save_data_to_parquet
from pyspark.sql.functions import sum, desc, first
DEFAULT_LIMIT = 10

def top_campaigns_sql(spark):
    result = spark.sql(
      f"""
      SELECT DISTINCT campaignId, sum(billingCost) as revenue FROM target
      WHERE isConfirmed = true
      GROUP BY campaignId
      ORDER BY revenue DESC
      LIMIT {DEFAULT_LIMIT}
      """
    )
    # save_data_to_parquet(result, './data/test/top_campaigns_sql')
    save_data_to_parquet(result, './output/top_campaigns/sql')
    #result.show()
    return result


def top_campaigns_spark_df(df):
    top_campaigns = (df.where('isConfirmed = true')
        .groupBy('campaignId')
        .agg(sum('billingCost').alias('revenue'))
        .orderBy(desc('revenue'))
        .limit(DEFAULT_LIMIT)
        .select('campaignId', 'revenue'))
    # save_data_to_parquet(top_campaigns, './data/test/top_campaigns_df')
    save_data_to_parquet(top_campaigns, './output/top_campaigns/spark_df')
    # top_campaigns.show()
    return top_campaigns