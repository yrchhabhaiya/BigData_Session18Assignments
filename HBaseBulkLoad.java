package assignment2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


@SuppressWarnings({ "unused", "deprecation" })
public class HBaseBulkLoad {

		public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {  
			
			final String COLUMN_FAMILY1 = "details";
			
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String line = value.toString();
				String[]parts=line.split(",");
				String rowKey = parts[0];
			
				//	The line is splitting the file records into parts wherever it is comma (‘,’) separated, and the first column are considered as rowKey.
				ImmutableBytesWritable HKey = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

				//Here the row key is first converted to Bytes as Hbase understand its data as Bytes, and also object is created as ImmutableBytesWriteable
				Put HPut = new Put(Bytes.toBytes(rowKey));
			
				//This will write the rowKey values into Hbase while creating an object.
				//Here the fields of tables inside Hbase is are stated to be written
				HPut.add(Bytes.toBytes(COLUMN_FAMILY1), Bytes.toBytes("name"), Bytes.toBytes(parts[1]));
				HPut.add(Bytes.toBytes(COLUMN_FAMILY1), Bytes.toBytes("location"), Bytes.toBytes(parts[2]));
				HPut.add(Bytes.toBytes(COLUMN_FAMILY1), Bytes.toBytes("age"), Bytes.toBytes(parts[3]));
			
				context.write(HKey,HPut);
				//first we are creating instance PUT with 1st field as row key,
			}  
		}

		
		public static void main(String[] args) throws Exception {
			Configuration conf = HBaseConfiguration.create();
			
			String inputPath = args[0];
			
			HTable table=new HTable(conf,args[2]);
			
			conf.set("hbase.mapred.outputtable", args[2]);
			
			Job job = new Job(conf,"HBase_Bulk_loader");  
			
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			
			job.setSpeculativeExecution(false);
			job.setReduceSpeculativeExecution(false);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(HFileOutputFormat.class);
			
			job.setJarByClass(HBaseBulkLoad.class);
			job.setMapperClass(HBaseBulkLoad.BulkLoadMap.class);
			
			FileInputFormat.setInputPaths(job, inputPath);
			TextOutputFormat.setOutputPath(job, new Path(args[1]));
			
			HFileOutputFormat.configureIncrementalLoad(job, table);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		
}