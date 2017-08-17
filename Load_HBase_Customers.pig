raw_data = LOAD '/home/acadgild/dataset/customers.dat' USING PigStorage(',') AS (
           id:chararray,
           name:chararray,
           location:chararray,
           age:int
);

STORE raw_data INTO 'hbase://customer' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'details:name
 details:location
 details:age'
);

