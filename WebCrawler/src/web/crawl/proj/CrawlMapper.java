package web.crawl.proj;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;


public class CrawlMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	Set<String> set = new HashSet<String>();
	
   @Override
    public void map(LongWritable key, Text url, Context context)
          throws IOException, InterruptedException {
       
	   try{
        if(set.contains(url.toString())!= true)  
         {
    	//initiate the HBase
 	    Configuration conf = HBaseConfiguration.create();
 	  
 	    
 	    //connect to  the table
 	    HTable hTable = new HTable(conf, "demo");
 	    
    	Get gt = new Get(Bytes.toBytes(url.toString()));
		    //get the result
		Result rs = hTable.get(gt);
	   if(rs.isEmpty())  
	   {
	   set.add(url.toString());
	   Document doc;
	   //read download the contents of the URL
	   doc = Jsoup.connect(url.toString()).timeout(0).get() ;
       System.out.println(url.toString());
	    
		//save it as bytes of data
	    byte[] data = Bytes.toBytes(doc.toString());
		
	    //create the new row to be inserted 
	    //TODO send the rowname and column name and all the other details to make sure the reducer gets hold of the data
	    Put p = new Put(Bytes.toBytes(url.toString()));

	    //insert the byte stream of the html into the HBase
	    //Put add(byte[] family, byte[] qualifier, byte[] value)
	    String pageNameGUID = UUID.randomUUID().toString();
	    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("page"+pageNameGUID),data);
	    hTable.put(p);
	    Text page = new Text();
	   
	    
	    FileSplit fileSplit = (FileSplit)context.getInputSplit();
	    
	    String filePath = fileSplit.getPath().toString();
	    System.out.println(filePath);
	    String filename = fileSplit.getPath().getName();
	    System.out.println(filePath);
	    page.set("page"+pageNameGUID+","+filePath+","+filename);
	    
	    context.write(url, page);
	    System.out.println("%%%MAPPER" +"page"+pageNameGUID);
	    System.out.println("%%%MAPPER DONE%%%");
	  
	   ;
			 }
	   else
	   {
		   System.out.println("URL Already Added");
	   }

	   }
        else{
     	   System.out.println("SEED files has duplicates");

        }
	   }
   catch (IOException e) {
			// TODO Auto-generated catch block
				
			}	    
	   
	   
	   
	   
	   
	   
	   /* Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("demo"));
        Scan scan = new Scan();
        ResultScanner res = table.getScanner(scan);
        

        for(Result r: res)
        {
        	System.out.println(r);
        }*/
		//send file name to reducer
        //context.write(word, one);

    }

}