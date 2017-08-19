package assignment2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class RecreateTable {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		String tableName = "customer";
		String columnFamily1 = "details";
		
		//Creating HTableDescriptor object
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName)); 
		
		//Adding column families to the HBase table
		HColumnDescriptor hColDescriptor = new HColumnDescriptor(columnFamily1);
				
		tableDescriptor.addFamily(hColDescriptor);
		
		//Disabling and Dropping table if already exists
		if (admin.isTableAvailable(tableName)) {
			if (admin.isTableEnabled(tableName)) {
				admin.disableTable(tableName);
			}
			admin.deleteTable(tableName);
		}
		
		//Creating HBase Table
		admin.createTable(tableDescriptor);

	}
}
