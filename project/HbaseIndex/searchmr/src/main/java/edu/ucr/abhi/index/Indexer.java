package edu.ucr.abhi.index;

import edu.ucr.abhi.constants.Search;
import edu.ucr.abhi.pojos.Posting;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Indexer extends Configured implements Tool{
  private static final Log log = LogFactory.getLog(Indexer.class);

  @Override
  public int run(String[] args) throws Exception {
    
    String inputFile = args[0];
    log.info("Input :" + inputFile);
    Job job = Job.getInstance(getConf(), Search.IJOB + inputFile);
    
    job.setJarByClass(Indexer.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(inputFile));
    
    job.setMapperClass(IndexerM.class);
    job.setReducerClass(IndexerR.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Posting.class);
    
    String outputTable = getConf().get(Search.TABLE);
    log.info("Hbase Output : " + outputTable);
    TableMapReduceUtil.initTableReducerJob(
      outputTable,
      IndexerR.class,
      job
    );
    
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : -1;
  }
  public static void main(String[] args) throws Exception {
    int result;
    try{
      Properties properties = new Properties();
      properties.load(Indexer.class.getClassLoader().getResourceAsStream(Search.PROPS));

      Configuration conf = HBaseConfiguration.create();
      conf.set(Search.TABLE, properties.getProperty(Search.TABLE));
      conf.set(Search.ICF, properties.getProperty(Search.ICF));
      conf.set(Search.ICQ, properties.getProperty(Search.ICQ));
      conf.set(Search.IHREF, properties.getProperty(Search.IHREF));
      
      result= ToolRunner.run(conf, new Indexer(), args);
      if(0 == result)
      {
        log.info("Job completed Successfully...");
      }
      else
      {
        log.info("Job Failed...");
      }
    }
    catch(Exception exception)
    {
      exception.printStackTrace();
    }
    
  }
  
  
}
