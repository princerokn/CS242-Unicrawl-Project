package edu.ucr.abhi.search;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;

import edu.ucr.abhi.constants.Search;
import edu.ucr.abhi.pojos.Posting;

public class RankM extends TableMapper<FloatWritable, Posting>{
  
    private static final Log log = LogFactory.getLog(RankM.class);
    private static FloatWritable score = new FloatWritable();

    @Override
    protected void map(
        ImmutableBytesWritable key, 
        Result value,
        Context context
    ) throws IOException, InterruptedException {
        
        String cf = context.getConfiguration().get(Search.ICF);
        String cq = context.getConfiguration().get(Search.ICQ);

        String query = context.getConfiguration().get(Search.QUERY);
        String word = Bytes.toString(key.get());
        Set<String> q = new HashSet<String>();
        for (String s: query.split(" ")) q.add(s);
        if (Query.match(word, q)) {
            log.info(word + "matches with " + query);
            for (Posting posting : Posting.fromResult(cf, cq, value)) {
                System.out.println("Load : " + posting);
                score.set(1.0f/posting.getCount().get());
                context.write(score, posting);
            }
        }
    }
}
