import dlt

@dlt.table
def factstream_stg():
    df = spark.read.table("spotify_cata.silver.factstream")
 #   df.display()
    return df

dlt.create_streaming_table("factstream")

dlt.create_auto_cdc_flow(
    target = "factstream",
    source = "factstream_stg",
    keys = ["stream_id"],
    sequence_by = "stream_timestamp",
    stored_as_scd_type = 1,
    name = None,
    once = False
)
