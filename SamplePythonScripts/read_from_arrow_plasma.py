
#
# Simple program to test reading objects from Plasma memory store
#
# This program works along with the "" Java test program
#

import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np

client = plasma.connect("/tmp/plasma")

# Ways to create an object ID
# 20 bytes of value 1
# id = plasma.ObjectID(20 * b'\x01')
# 20 bytes of a known character (in this case, '1' which is hex 31)
# id = plasma.ObjectID(20 * b'1')
# or we can make a totally random ID using numpy
# id = plasma.ObjectID(np.random.bytes(20))

# Iterate through keys (which are the IDs)
ids = client.list()
for key in ids:
   print(key)

id_list = list(ids.keys())

# id = plasma.ObjectID(20 * b'\x01')
id = id_list[0]
[data] = client.get_buffers([id])
view = memoryview(data)
bytes(view)


# id = plasma.ObjectID(20 * b'\x02')
id = id_list[1]
[data] = client.get_buffers([id])
buffer = pa.BufferReader(data)
reader = pa.RecordBatchStreamReader(buffer)
table = reader.read_all()
table.column('int')
table.column('varchar')
table.column('boolean')
