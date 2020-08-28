
#
# Simple example to write out an Arrow file to disk and read it back in
#
# This is based on:
# https://arrow.apache.org/docs/python/ipc.html
# https://towardsdatascience.com/apache-arrow-read-dataframe-with-zero-memory-69634092b1a
#

import pyarrow as pa

dataA = [
    pa.array([1, 2, 3, 4]),
    pa.array(['foo', 'bar', 'baz', None]),
    pa.array([True, None, False, True])
]
dataB = [
    pa.array([11, 22, 33, 44]),
    pa.array(['foo', 'bar', 'baz', None]),
    pa.array([True, None, False, True])
]

batchA = pa.record_batch(dataA, names=['f0', 'f1', 'f2'])
batchB = pa.record_batch(dataB, names=['f0', 'f1', 'f2'])

print('Schema:\n')
print(batchA.schema)

# Write out the record to file
with pa.OSFile('data.arrow', 'wb') as sink:
    with pa.RecordBatchFileWriter(sink, batchA.schema) as writer:
        writer.write_batch(batchA)
        writer.write_batch(batchB)


# Read the data as memory mapped file
source = pa.memory_map('data.arrow', 'r')
table = pa.ipc.RecordBatchFileReader(source).read_all().column("f0")
print('\nTable read from memory-mapped file, column f0:\n')
print(table)
