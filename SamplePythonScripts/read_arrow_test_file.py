
#
# Python script which reads Arrow data from a file; will read the Arrow file written out by our sample "ArrowTestJava" application.
#
# This is based on:
# https://arrow.apache.org/docs/python/ipc.html
# https://towardsdatascience.com/apache-arrow-read-dataframe-with-zero-memory-69634092b1a
#
#

import pyarrow as pa

arrow_file_name = '.\\test.arrow'

print('Reading Arrow data file \"' + arrow_file_name + '\"')

# Read as memory mapped file from disk
source = pa.memory_map(arrow_file_name,'r')
# Read entire content
print('Reading entire file contents:')
table = pa.ipc.RecordBatchFileReader(source).read_all()
print(table['int'])
print(table['varchar'])
print(table['boolean'])
# Read one column
print('Reading one column:')
str_table = pa.ipc.RecordBatchFileReader(source).read_all().column("varchar")
print(str_table)


print('Reading Arrow data file \"' + arrow_file_name + '\"')

# Read as memory mapped file from disk
source = pa.memory_map(arrow_file_name,'r')
# Read entire content
print('Reading entire file contents:')
table = pa.ipc.RecordBatchFileReader(source).read_all()
print(table['int'])
print(table['varchar'])
print(table['boolean'])
# Here's one way to access the data:
# pl = table['int'].to_pylist()
# Read entire varchar column, over all batches
str_table = pa.ipc.RecordBatchFileReader(source).read_all().column("varchar")
print('All data from varchar column:')
print(str_table)
# Read one column from each batch
print('Reading varchar column from each batch individually:')
reader = pa.ipc.RecordBatchFileReader(source)
for i in range(reader.num_record_batches):
    print('Batch ' + str(i) + ':')
    batch = reader.get_batch(i)
    col = batch.column(1)
    # Here's one way to access the data:
    # pl = col.to_pylist()
    # pl[0]
    # pl[1]
    # ...
    # pl[8]
    # For formatted output:
    # for j in range(length of pl):
    #    print('{:.4f}'.format(pl[j]))
    print(col)
