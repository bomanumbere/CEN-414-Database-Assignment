import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.*;
import java.io.*;  
import java.util.Scanner;  
import java.io.BufferedReader;
import java.io.FileReader;


public class Script {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();// Instantiating Configuration class
        try {           
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin hAdmin = conn.getAdmin();


            HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf("Total population supplied by water supply industry"));
            hTableDesc.addFamily(new HColumnDescriptor("Country"));
            hTableDesc.addFamily(new HColumnDescriptor("Year"));
            hTableDesc.addFamily(new HColumnDescriptor("Value"));
            hTableDesc.addFamily(new HColumnDescriptor("2010"));
            hTableDesc.addFamily(new HColumnDescriptor("2011"));
            hTableDesc.addFamily(new HColumnDescriptor("2012"));
            hTableDesc.addFamily(new HColumnDescriptor("2013"));
            hTableDesc.addFamily(new HColumnDescriptor("2014"));
            hTableDesc.addFamily(new HColumnDescriptor("2015"));
            hTableDesc.addFamily(new HColumnDescriptor("2016"));
            hTableDesc.addFamily(new HColumnDescriptor("2017"));
            hTableDesc.addFamily(new HColumnDescriptor("2018"));
            hTableDesc.addFamily(new HColumnDescriptor("2019"));

            hAdmin.createTable(hTableDesc);
            System.out.println("Table created Successfully...");


            Table table = conn.getTable(TableName.valueOf("Total population supplied by water supply industry"));
            HTable htable = new HTable(conf, "Total population supplied by water supply industry"); //Instantiating htable class
            BufferedReader csvReader = new BufferedReader(new FileReader("water.csv"));

            int count = 1;
            String row =  "";
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                String rowStr = "row" + String.valueOf(count);

                Put p = new Put(Bytes.toBytes(rowStr));// Accepts a Row name

                p.addColumn(Bytes.toBytes("Country"),Bytes.toBytes("Country"), Bytes.toBytes(data[0]));
                p.addColumn(Bytes.toBytes("Year"),Bytes.toBytes("Year"), Bytes.toBytes(data[1]));
                p.addColumn(Bytes.toBytes("Value"),Bytes.toBytes("Value"), Bytes.toBytes(data[2]));
                p.addColumn(Bytes.toBytes("2010"),Bytes.toBytes("2010"), Bytes.toBytes(data[3]));
                p.addColumn(Bytes.toBytes("2011"),Bytes.toBytes("2011"), Bytes.toBytes(data[4]));
                p.addColumn(Bytes.toBytes("2012"),Bytes.toBytes("2012"), Bytes.toBytes(data[5]));
                p.addColumn(Bytes.toBytes("2013"),Bytes.toBytes("2013"), Bytes.toBytes(data[6]));
                p.addColumn(Bytes.toBytes("2014"),Bytes.toBytes("2014"), Bytes.toBytes(data[7]));
                p.addColumn(Bytes.toBytes("2015"),Bytes.toBytes("2015"), Bytes.toBytes(data[8]));
                p.addColumn(Bytes.toBytes("2016"),Bytes.toBytes("2016"), Bytes.toBytes(data[9]));
                p.addColumn(Bytes.toBytes("2017"),Bytes.toBytes("2017"), Bytes.toBytes(data[10]));
                p.addColumn(Bytes.toBytes("2018"),Bytes.toBytes("2018"), Bytes.toBytes(data[11]));
                p.addColumn(Bytes.toBytes("2019"),Bytes.toBytes("2019"), Bytes.toBytes(data[12]));
                  

            

                table.put(p);
                count++;
                
            }
            csvReader.close();
            table.close();
            conn.close();

        } 
        catch (IOException e) {
            e.printStackTrace();     
        }
    }
}