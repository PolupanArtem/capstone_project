def save_data_to_parquet(df, path, mode='overwrite'):
    df.write.parquet(path, mode=mode)