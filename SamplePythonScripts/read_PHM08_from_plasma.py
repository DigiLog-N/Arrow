
#
# Commands for reading record batches of PHM08 data from Apache Plasma in-memory data store
#
# This script shows sample commands for reading data from Plasma. Works with PHM08 data that has been
# written to Plasma by the Java program "PHM08_to_Plasma". Data for each vehicle is saved in its own
# record batch. The schema for these record batches is as follows:
#
# unit: int32
# time_cycles: int32
# op1: float
# op2: float
# op3: float
# sensor01: float
# sensor02: float
# sensor03: float
# sensor04: float
# sensor05: float
# sensor06: float
# sensor07: float
# sensor08: float
# sensor09: float
# sensor10: float
# sensor11: float
# sensor12: float
# sensor13: float
# sensor14: float
# sensor15: float
# sensor16: float
# sensor17: float
# sensor18: float
# sensor19: float
# sensor20: float
# sensor21: float
#
# John P. Wilson, Erigo Technologies
#

import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np

client = plasma.connect("/tmp/plasma")
client.list()
idstr = 'PHM08_unit_1*_b00001'
idbytes = idstr.encode()
id = plasma.ObjectID(idbytes)
print(id)

[data] = client.get_buffers([id])
buffer = pa.BufferReader(data)
reader = pa.RecordBatchStreamReader(buffer)
# Show the schema for this record batch (contains the channel names and types)
print(reader.schema)

# Each record batch contains data for 1 unit from the PHM08 dataset.
# Can iterate through the batches and get the columns of data for each batch
batch_num = 0
while True:
    try:
        batch_num = batch_num + 1
        # Examine this record batch
        # https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html#pyarrow.RecordBatch
        batch = reader.read_next_batch()
        print('\nReading record batch number %d' % (batch_num))
        print('   number of columns = %d' %(batch.num_columns))
        print('   number of rows = %d' % (batch.num_rows))
        print('   schema:')
        print(batch.schema)
        # Display one of the columns of data
        print('Data for the second channel (column 1):')
        col = batch.column(1)
        print(col)
        # Another way to access the data:
        print('All the data for the second channel:')
        pl = col.to_pylist()
        for i in range(len(pl)):
            print('{:.4f}'.format(pl[i]))
    except StopIteration:
        print("\nFinished reading the record batches!")
        break
