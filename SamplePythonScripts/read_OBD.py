
#
# Sample Python script demonstrating some Arrow API calls for reading an Arrow file
#
# For further information:
# https://arrow.apache.org/docs/python/ipc.html
#

import pyarrow as pa

arrow_file_name = '.\\dailyRoutes.arrow'

source = pa.memory_map(arrow_file_name,'r')

# Read entire content
table = pa.ipc.RecordBatchFileReader(source).read_all()
print(table['ENGINE_LOAD'])
pl = table['ENGINE_LOAD'].to_pylist()

reader = pa.ipc.RecordBatchFileReader(source)

reader.num_record_batches

batch = reader.get_batch(0)

col = batch.column(10)

col

batch = reader.get_batch(1)
