import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField,
                             StringType, FloatType)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.expressions import EqualTo
import pyarrow as pa

os.environ["PYICEBERG_HOME"] = os.getcwd()
catalog = load_catalog(name='pyarrowtest')

print(catalog.properties)

# Create an Iceberg schema for the ingested data
schema = Schema(
    NestedField(1, "pos_id", StringType(), required=True),
    NestedField(2, "total_amount", FloatType(), required=True),
    NestedField(3, "device_make", StringType(), required=True),
    identifier_field_ids=[1]  # 'device_id' is the primary key
)

# Create a partition specification with device_id as the partition key
partition_spec = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="pos_id")
)

# Create a namespace and an iceberg table
catalog.create_namespace_if_not_exists('pos_ns')
pos_table = catalog.create_table_if_not_exists(
    identifier='pos_ns.pos_sales',
    schema=schema,
    partition_spec=partition_spec
)

# Create the first batch of in-memory arrow records
initial_data = pa.table([
    pa.array(['wi27', 'wi35', 'wi86']),
    pa.array([350.5, 564.32, 101.85]),
    pa.array(['INGENICO', 'VERIFONE', 'MSWIPE'])
], schema=schema.as_arrow())

# Insert initial data
pos_table.overwrite(initial_data)

# Print a Pandas dataframe representation of the table data
print("\nInsert operation completed")
print(pos_table.scan().to_pandas())

# Create an UPSERT batch of Arrow records where one fot he device_make is changed
upsert_data = pa.table([
   pa.array(['wi27', 'wi35', 'wi86']),
    pa.array([350.5, 564.32, 101.85]),
    pa.array(['INGENICO', 'VERIFONE', 'PINELABS'])
], schema=schema.as_arrow())

# UPSERT changed data
try:
    join_columns = ["pos_id"]
    upsert_result = pos_table.upsert(upsert_data.select(["pos_id", "total_amount", "device_make"]))
except Exception as e:
    print(e)

print("\nUpsert operation completed")
print(pos_table.scan().to_pandas())
print("\n")
print(f"Rows Updated: {upsert_result.rows_updated}")

# Filter columns
print("\nFilter records with device_make == PINELABS ")
print(pos_table.scan(row_filter=EqualTo('device_make', 'PINELABS')).to_pandas())
print("\n")

# Delete row
pos_table.delete(delete_filter=EqualTo('device_make', 'INGENICO'))
print("\n After Delete")
print(pos_table.scan().to_pandas())

# DuckDB
con = pos_table.scan().to_duckdb(table_name='all_sales')
duck_result = con.execute(""" 
    SELECT pos_id, total_amount, device_make
    FROM all_sales
""").fetchall()
print("\nDuckDB SELECT * ==>\n")
print(duck_result)

# PyIceberg inspection API
import pprint

# Inspect Snapshots
# print("\nInspection API ==> SNAPSHOTS()")
# print("\n*****************************")
# pprint.pp(pos_table.inspect.snapshots())

# Inspect Partitions
# print("\nInspection API ==> PARTITIONS()()")
# print("\n*****************************")
# pprint.pp(pos_table.inspect.partitions())

# Inspect Files
# print("\nInspection API ==> FILES()")
# print("\n*****************************")
# pprint.pp(pos_table.inspect.files())
