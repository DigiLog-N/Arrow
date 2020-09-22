
#
# Simple program to test reading objects from Plasma memory store (located at "/tmp/plasma")
#
# This program works along with the "ArrowPlasmaTestJava" Java test program
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

# NOTE: The id's in the list aren't necessarily in the time-ordered order that Plasma received the objects in
# id = id_list[0]
id = plasma.ObjectID(20 * b'\x01')
print(id)
[data] = client.get_buffers([id])
view = memoryview(data)
print(bytes(view))

id = plasma.ObjectID(20 * b'\x02')
print(id)
[data] = client.get_buffers([id])
buffer = pa.BufferReader(data)
reader = pa.RecordBatchStreamReader(buffer)
table = reader.read_all()
print(table.column('int'))
print(table.column('varchar'))
print(table.column('boolean'))
