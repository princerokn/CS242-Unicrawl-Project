package edu.ucr.abhi.index;

import edu.ucr.abhi.constants.Search;
import edu.ucr.abhi.pojos.*;

import java.io.IOException;
import java.util.HashMap;

import com.google.gson.Gson;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class IndexerR  extends TableReducer<Text, Posting, ImmutableBytesWritable> {
  
  private static final Log log = LogFactory.getLog(IndexerM.class);
  private static final Gson gson = new Gson();
  
  @Override
  protected void reduce(
  Text word, 
  Iterable<Posting> postings,
  Context context
  ) throws IOException, InterruptedException {

    Integer count;
    String root, href, title;
    HashMap<String, HashMap<String, Integer>> m2;
    HashMap<String, Integer> m1;
    HashMap<String, HashMap<String, HashMap<String, Integer>>> map = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();
    for (Posting posting: postings) {
      root = Bytes.toString(posting.getRoot().getBytes());
      href = Bytes.toString(posting.getHyperlink().getBytes());
      title = Bytes.toString(posting.getTitle().getBytes());
      count = posting.getCount().get();
      if (map.containsKey(root)) {
        m2 = map.get(root);
        if (m2.containsKey(title)) {
          m1 = m2.get(title);
          if (m1.containsKey(href)) {
            m1.put(href, m1.get(href) + count);
          } else {
            m1.put(href, count);
          }
        } else {
          m1 = new HashMap<String, Integer>();
          m1.put(href, count);
          m2.put(title, m1);
        }
      } else {
        m1 = new HashMap<String, Integer>();
        m1.put(href, count);
        m2 = new HashMap<String, HashMap<String, Integer>>();
        m2.put(title, m1);
        map.put(root, m2);
      }
    }
    
    // TODO : this is only insert , need to handle update case where the key already is put
    ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(word.toString()));
    Put put = new Put(Bytes.toBytes(word.toString()));
    String json = gson.toJson(map);
    log.info(word.toString() + " is mapped to " + map.keySet().size() + "number of root pages");
    String cf = context.getConfiguration().get(Search.ICF);
    String cq = context.getConfiguration().get(Search.ICQ);
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes(json));
    context.write(key, put);
  }
}