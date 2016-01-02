package web.crawl.proj;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.validator.routines.UrlValidator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

public class CrawlReducer<KEY> extends Reducer<KEY, Text, Text, Text> 
{
	int x=0;
	
	  @Override
	    public void reduce(KEY key,Iterable<Text> value,Context context) throws InterruptedException 
	  {
		  try{
		    StringBuilder urlCollection = new StringBuilder();
		    Configuration conf = HBaseConfiguration.create();
		    HashSet<String> uniqueURLSet = new HashSet<String>();
		    HTable hTable = new HTable(conf, "demo");
		    System.out.println("DOING REDUCER");
		    //get the table
		    Get gt = new Get(Bytes.toBytes(key.toString()));
		    //get the result
		    Result rs = hTable.get(gt);
		    Iterator it = value.iterator();
		    String pageName ="";
		    while(it.hasNext())
		    {
		     pageName = it.next().toString();
		    }
		   //read the stream of data and save it as HTML file -> save it in a doc using Jsoup and find 
		    //all the URLs in them
		    String fGUID = UUID.randomUUID().toString();
		    
		    if(pageName!="")
		    {
		    	Document doc = Jsoup.parse(Bytes.toString(rs.getValue(Bytes.toBytes("cf1"), Bytes.toBytes(pageName.split(",")[0]))));
		    	Elements urlList = doc.select("a[href]");
		    	Iterator urls = urlList.iterator();
		    	UrlValidator urlValidator = new UrlValidator();
	        	for(Element e:urlList)
		        {
		        	String anchorRegex = "<\\s*a\\s+.*?href\\s*=\\s*\"(\\S*?)\".*?>";
		        	Pattern anchorPattern = Pattern.compile(anchorRegex, Pattern.CASE_INSENSITIVE);
		        	String content = e.toString();
		        	Matcher matcher = anchorPattern.matcher(content);	
		        	while(matcher.find()) {
		        		//if(urlValidator.isValid(matcher.group(1)))
		        		if(urlValidator.isValid(matcher.group(1).toString()))
		        		{
		        			//fos.write(Bytes.toBytes(matcher.group(1)));//(urls.next()));
		        			String temp =matcher.group(1);
		        			temp = temp.replace("&amp;", "&");
		        			uniqueURLSet.add(temp);
		        			System.out.println(temp);
		        		}
		        		else {
		        			String url = key.toString();
		        			if(url.isEmpty()!=true)
		        			{			        			
		        				//url = url.substring(0, url.length()-1);
		        				String path = url.split("/")[0];
		        				System.out.println("truncated url dude: "+path+"//"+url.split("/")[2]);
		        				url = path+"//"+url.split("/")[2];
		        			
			        		if(urlValidator.isValid(url+matcher.group(1).toString()))
			        		{
			        		//fos.write(Bytes.toBytes(key.toString()+matcher.group(1)));
			        			String temp =url+matcher.group(1);
			        			temp = temp.replace("&amp;", "&");
			        			uniqueURLSet.add(temp);
			        	     System.out.println(temp);
			        		}
		        		}
						}
		        	}
		        		        	
		        }
	        	String filePath = pageName.split(",")[1];
	        	String fileName = pageName.split(",")[2];
	        	System.out.println(filePath.substring(5, filePath.length()) +" "+ fileName);
	        	File inputFile = new File(filePath.substring(5, filePath.length()));
	        	File inputFile1 = new File(filePath.substring(5, filePath.length())+"~");

	        	if (inputFile.exists()){
	        		inputFile.delete();
	        		if(inputFile1.exists())
	        		{
	        			inputFile1.delete();
	        		}
	        	 }  
	        	
	        	filePath = filePath.substring(5,(filePath.length()-fileName.length()));
                System.out.println(filePath);
                System.out.println(filePath+fGUID+".html");
	        	FileOutputStream fos = new FileOutputStream(filePath+fGUID+".html",true);   
	        	Iterator set = uniqueURLSet.iterator();
	        	System.out.println("#############$$$$$$$$$$$$$$%%%%%%%%%%%%"+uniqueURLSet.toString());
	        	while(set.hasNext())
	        	{
		        	urlCollection.append(set.next()+"\n");
		        	set.remove();
	        	}

	        	try {
				    //.write(Bytes.toBytes(urls.next().toString()));//(urls.next()));

	        	fos.write(Bytes.toBytes(urlCollection.toString()));
	        	}
				finally {
				  fos.close();
				}
		    }
	    	Text k = new Text();
	    	Text v = new Text();
	    	k.set("");
	    	v.set("");
	    	context.write(k,v);
		  }
		  catch (Exception e) {
				// TODO Auto-generated catch block
					System.out.println(e.toString());
				}
	}
	   
		  
	    
}
