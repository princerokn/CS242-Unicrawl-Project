package edu.ucr.abhi.search;
import java.io.OutputStream;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ucr.abhi.constants.Search;
import edu.ucr.abhi.pojos.Posting;


public class Rank extends Configured implements Tool{
    
    private static final Log log = LogFactory.getLog(Rank.class);
    @Override
    public int run(String[] args) throws Exception {
        
        String query = getConf().get(Search.QUERY);
        Job job = Job.getInstance(getConf() , Search.RJOB + query);
        job.setJarByClass(Rank.class);
        String table = getConf().get(Search.TABLE);

        log.info("Read hbase table with name :" + table);

        TableMapReduceUtil.initTableMapperJob(
            table,      // input table
            Query.getScan(),             // Scan instance to control CF and attribute selection
            RankM.class,   // mapper class
            FloatWritable.class,             // mapper output key
            Posting.class,             // mapper output value
            job
        );
        
        job.setReducerClass(RankR.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : -1;
    }
    public static void main(String[] args) throws Exception {
        int result;
        try{
            Properties properties = new Properties();
            properties.load(Rank.class.getClassLoader().getResourceAsStream(Search.PROPS));
      
            Configuration conf = HBaseConfiguration.create();
            conf.set(Search.TABLE, properties.getProperty(Search.TABLE));
            conf.set(Search.ICF, properties.getProperty(Search.ICF));
            conf.set(Search.ICQ, properties.getProperty(Search.ICQ));

	    StringBuilder sb = new StringBuilder();
	    for (int i=1;i<args.length;++i) {
		    sb.append(args[i]);
		    sb.append(" ");
	    }
	    String query = sb.toString();
            conf.set(Search.QUERY, query);
            log.info("Query :" + query);
            result= ToolRunner.run(conf, new Rank(), args);
            if(0 == result)
                log.info("Job completed Successfully...");
            else
                log.error("Job Failed...");
        }
        catch(Exception exception)
        {
            exception.printStackTrace();
        }
        
    }
    
    
}
