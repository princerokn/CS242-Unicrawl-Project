package edu.ucr.abhi.search;

import java.io.IOException;

import com.google.gson.Gson;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ucr.abhi.pojos.Output;
import edu.ucr.abhi.pojos.Posting;

public class RankR extends Reducer<FloatWritable, Posting, FloatWritable, Text>{
        
    public static final Log log = LogFactory.getLog(RankR.class);
    private static final Gson gson = new Gson();
    private static final Text result = new Text();

    @Override
    protected void reduce(FloatWritable key, Iterable<Posting> value, Context context)
    throws IOException, InterruptedException {
        for (Posting p: value) {
            result.set(gson.toJson(new Output(0, Bytes.toString(p.getTitle().getBytes()), Bytes.toString(p.getRoot().getBytes()), p.getCount().get())));
            context.write(null, result);
        }
    }
}
