from pyiceberg.catalog import load_catalog
import daft
import os

os.environ["PYICEBERG_HOME"] = os.getcwd()
catalog = load_catalog(name='pyarrowtest')

postab = catalog.load_table("pos_ns.pos_sales")

df = daft.read_iceberg(postab)
df.show()
