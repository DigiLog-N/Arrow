import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np

client = plasma.connect("/tmp/plasma")
client.list()
id = plasma.ObjectID(20 * b'\x01')
print(id)

[data] = client.get_buffers([id])
buffer = pa.BufferReader(data)
reader = pa.RecordBatchStreamReader(buffer)

# Each record batch contains data for 1 unit from the PHM08
# Can iterate through the batches and get the columns of data for each batch
# BATCH 1 - unit 1
batch = reader.read_next_batch()
batch
batch.column(0)
batch.column(1)
batch.column(2)
# BATCH 2 - unit 2
batch = reader.read_next_batch()
batch.column(0)
batch.column(1)
batch.column(2)
