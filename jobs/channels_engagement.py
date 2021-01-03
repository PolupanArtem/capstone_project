from pyspark.sql.functions import sum, desc, first, countDistinct, max
from save_to_parquet import save_data_to_parquet

def channels_engagement_sql(spark):
    result = spark.sql(
      """
      SELECT campaignId, first(channelId) as channelId, max(sessionCount) AS maxSessions
      FROM (
        SELECT campaignId, channelId, count(distinct sessionId) AS sessionCount
        FROM target
        GROUP BY campaignId, channelId
        ORDER BY campaignId, sessionCount DESC
      )
      GROUP BY campaignId
      ORDER BY maxSessions DESC
      """
    )
    # save_data_to_parquet(result, './data/test/channels_engagement_spark_sql')
    save_data_to_parquet(result, './output/channels_engagement/sql')
    # result.show()
    return result


def channels_engagement_spark_df(df):
    channels_engagement_df = (
      df.groupBy('campaignId', 'channelId')
        .agg(countDistinct('sessionId').alias('sessionCount'))
        .orderBy('campaignId', desc('sessionCount'))
        .select('campaignId', 'channelId', 'sessionCount')
      )

    channels_engagement_df = (channels_engagement_df.groupBy('campaignId')
        .agg(max('sessionCount').alias('maxSessions'), first('channelId').alias('channelId'))
        .orderBy(desc('maxSessions'))
        .select('campaignId', 'channelId', 'maxSessions'))

    # save_data_to_parquet(channels_engagement_df, './data/test/channels_engagement_spark_df')
    save_data_to_parquet(channels_engagement_df, './output/channels_engagement/spark_df')
    # channels_engagement_df.show()
    return channels_engagement_df