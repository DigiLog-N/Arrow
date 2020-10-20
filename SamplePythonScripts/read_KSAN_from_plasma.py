
#
# Read KSAN data from Apache Plasma in-memory data store
#
# This script shows sample commands for reading data from Plasma.
#
# Works with KSAN data that has been written to Plasma by the CT2Arrow.
#
# The schema for these record batches is as follows:
#
# ct_timestamp: double
# Date_Time: string
# Station_ID: string
# altimeter_set_1: float
# air_temp_set_1: float
# dew_point_temperature_set_1: float
# relative_humidity_set_1: float
# wind_speed_set_1: float
# wind_direction_set_1: float
# wind_gust_set_1: float
# sea_level_pressure_set_1: float
# weather_cond_code_set_1: int32
# cloud_layer_3_code_set_1: int32
# pressure_tendency_set_1: int32
# precip_accum_one_hour_set_1: float
# precip_accum_three_hour_set_1: float
# cloud_layer_1_code_set_1: int32
# cloud_layer_2_code_set_1: int32
# precip_accum_six_hour_set_1: float
# precip_accum_24_hour_set_1: float
# visibility_set_1: float
# metar_remark_set_1: string
# metar_set_1: string
# air_temp_high_6_hour_set_1: float
# air_temp_low_6_hour_set_1: float
# peak_wind_speed_set_1: float
# ceiling_set_1: float
# pressure_change_code_set_1: int32
# air_temp_high_24_hour_set_1: float
# air_temp_low_24_hour_set_1: float
# peak_wind_direction_set_1: float
# dew_point_temperature_set_1d: float
# cloud_layer_1_set_1d: int32
# cloud_layer_3_set_1d: int32
# cloud_layer_2_set_1d: int32
# wind_chill_set_1d: float
# weather_summary_set_1d: int32
# wind_cardinal_direction_set_1d: int32
# pressure_set_1d: float
# sea_level_pressure_set_1d: float
# heat_index_set_1d: float
# weather_condition_set_1d: int32
#
# John P. Wilson, Erigo Technologies
#

import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np
import sys

# Connect to Plasma
client = plasma.connect("/tmp/plasma")
# This returns a dictionary of the objects currently in the Plasma store
# dict = client.list()

# Fetch one object from Plasma
# idstr = 'KSAN*********_b00001'
idstr = sys.argv[1]
idbytes = idstr.encode()
id = plasma.ObjectID(idbytes)

# Read data from the Plasma object
# Each Plasma object written out from CT2Arrow only contains 1 record batch
# Examine the data from this record batch
# (see https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html#pyarrow.RecordBatch)
[data] = client.get_buffers([id])
buffer = pa.BufferReader(data)
reader = pa.RecordBatchStreamReader(buffer)
batch = reader.read_next_batch()
print('number of columns = %d' %(batch.num_columns))
print('number of rows = %d' % (batch.num_rows))
print('schema:')
print(batch.schema)
col = batch.column(0)
pl_ct_timestamp = col.to_pylist()
col = batch.column(1)
date_time = col.to_pylist()
col = batch.column(4)
air_temp = col.to_pylist()
for i in range(len(pl_ct_timestamp)):
    print('{:.3f}\t{:s}\t{:.4f}'.format(pl_ct_timestamp[i],date_time[i],air_temp[i]))

