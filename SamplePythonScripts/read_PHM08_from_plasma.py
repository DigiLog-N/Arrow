
#
# Read PHM08 data from Apache Plasma in-memory data store
#
# This script shows sample commands for reading data from Plasma.
#
# Works with PHM08 data that has been written to Plasma by the CT2Arrow.
#
# Usage:
#    python3 read_PHM08_from_plasma.py <plasma store> <plasma object ID>
#    e.g.  python3  read_PHM08_from_plasma.py  /tmp/plasma  PHM08********_b00001
#
# The schema for these record batches is as follows:
#
# ct_timestamp: double
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
import sys

# Connect to Plasma
client = plasma.connect(sys.argv[1])
# This returns a dictionary of the objects currently in the Plasma store
# dict = client.list()

# Fetch one object from Plasma
# idstr = 'PHM08********_b00001'
idstr = sys.argv[2]
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
pl_unit = col.to_pylist()
col = batch.column(2)
pl_cycle = col.to_pylist()
col = batch.column(3)
pl_op1 = col.to_pylist()
col = batch.column(6)
pl_sensor01 = col.to_pylist()
col = batch.column(26)
pl_sensor21 = col.to_pylist()
for i in range(len(pl_ct_timestamp)):
    print('{:.3f},{:d},{:d},{:.4f},{:.4f},{:.4f}'.format(pl_ct_timestamp[i],pl_unit[i],pl_cycle[i],pl_op1[i],pl_sensor01[i],pl_sensor21[i]))

