
#
# Simple example to write out an Arrow file to disk and read it back in
#
# This is based on:
# Plasma code: https://arrow.apache.org/docs/python/plasma.html
# https://arrow.apache.org/docs/python/ipc.html
# https://towardsdatascience.com/apache-arrow-read-dataframe-with-zero-memory-69634092b1a
#

import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np

dataA = [
    pa.array([1, 2, 3, 4]),
    pa.array(['foo', 'bar', 'baz', None]),
    pa.array([True, None, False, True])
]

batchA = pa.record_batch(dataA, names=['f0', 'f1', 'f2'])

print('Schema:\n')
print(batchA.schema)

#
# Write record to plasma
#
# Create the Plasma object from the PyArrow RecordBatch. Most of the work here
# is done to determine the size of buffer to request from the object store.
object_id = plasma.ObjectID(np.random.bytes(20))
mock_sink = pa.MockOutputStream()
stream_writer = pa.RecordBatchStreamWriter(mock_sink, batchA.schema)
stream_writer.write_batch(batchA)
stream_writer.close()
data_size = mock_sink.size()
client = plasma.connect("/tmp/plasma")
buf = client.create(object_id, data_size)
# Write the PyArrow RecordBatch to Plasma
stream = pa.FixedSizeBufferWriter(buf)
stream_writer = pa.RecordBatchStreamWriter(stream, batchA.schema)
stream_writer.write_batch(batchA)
stream_writer.close()
# Seal the Plasma object
client.seal(object_id)

#
# Read the batch from Plasma
#
# Fetch the Plasma object
[data] = client.get_buffers([object_id])  # Get PlasmaBuffer from ObjectID
buffer = pa.BufferReader(data)
# Convert object back into an Arrow RecordBatch
reader = pa.RecordBatchStreamReader(buffer)
table = reader.read_all()
print('\nFrom plasma, column f0:\n')
print(table.column("f0"))
print('\nFrom plasma, column f1:\n')
print(table.column("f1"))
print('\nFrom plasma, column f2:\n')
print(table.column("f2"))
