Source: https://github.com/Teradata/kylo/tree/master/samples/sample-data/parquet
(Also Apache 2 license)

userdata[1-5].parquet: These are sample files containing data in PARQUET format.

-> Number of rows in each file: 1000
-> Column details:
```
column#		column_name		hive_datatype
=====================================================
1		registration_dttm 	timestamp
2		id 			int
3		first_name 		string
4		last_name 		string
5		email 			string
6		gender 			string
7		ip_address 		string
8		cc 			string
9		country 		string
10		birthdate 		string
11		salary 			double
12		title 			string
13		comments 		string
```


The singlefile and multifolder variants were generated from the original multifile
flavour by:
```
import pyarrow as pa
import pyarrow.parquet as pq
df = pq.read_table(".../parquet/multifile").to_pandas()
df.to_parquet('.../parquet/singlefile.parquet',index=False)
pq.write_to_dataset(pa.Table.from_pandas(df, preserve_index=False), partition_cols=["gender"], flavor="spark", compression="snappy", root_path=".../parquet/multifolder/")
```
