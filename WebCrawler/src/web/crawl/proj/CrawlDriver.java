package web.crawl.proj;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.corba.se.impl.oa.poa.ActiveObjectMap.Key;

public class CrawlDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out
                  .printf("Two parameters are required for DriverNLineInputFormat- <input dir> <output dir>\n");
            return -1;
        }
        List<String> fileList = new ArrayList<String>();

        Job job = new Job(getConf());
        job.setJobName("Distributed Web Crawler");
        job.setJarByClass(CrawlDriver.class);

      //the reduce output
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      //the map output
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      File folder = new File(args[0]);
      File[] listOfFiles = folder.listFiles();

          for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
            	fileList.add(listOfFiles[i].toString()); 
            } 
          }
		FileInputFormat.addInputPaths(job, StringUtils.join(fileList, ','));//new Path(args[0]));        
        FileUtils.deleteDirectory(new File(args[1]));
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CrawlMapper.class);
        job.setReducerClass(CrawlReducer.class);
        job.setNumReduceTasks(1);
        /*if(countLines(args[0])>0)
        {
        job.setNumReduceTasks(countLines(args[0]));
        }
        else
        {
            job.setNumReduceTasks(0);
        }*/
        //job.setNumReduceTasks(0);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
    
    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
    public static void main(String[] args) throws Exception {
    	int exitCode =0;
    	/*for (int i = 0; i < 2; i++) {
    		exitCode  = ToolRunner.run(new Configuration(), new CrawlDriver(), args);
		}*/
    	long startTime = System.currentTimeMillis();
    	while ((System.currentTimeMillis()-startTime)< 360*60*1000){
    		exitCode  = ToolRunner.run(new Configuration(), new CrawlDriver(), args);

    		// do stuff
    	}
        System.exit(exitCode);
    }
}