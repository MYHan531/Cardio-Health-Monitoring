from lxml import etree
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

context = etree.iterparse("../data/raw/apple_health_export/export.xml", events=('end',), tag='Record')
records = []
batch_size = 100_000
out_path = "../data/raw/apple_health_records.parquet"
writer = None

for _, elem in context:
    record = elem.attrib
    records.append(record)
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]

    if len(records) >= batch_size:
        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df)
        if writer is None:
            writer = pq.ParquetWriter(out_path, table.schema, compression='zstd')
        writer.write_table(table)
        records.clear()

# Write any remaining rows
if records:
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    if writer is None:
        writer = pq.ParquetWriter(out_path, table.schema, compression='zstd')
    writer.write_table(table)
    records.clear()

if writer:
    writer.close()
