package edu.ucr.abhi.pojos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Posting implements WritableComparable<Posting> {
    
    private Text root;
    private Text hyperlink;
    private Text title;
    private IntWritable count;
    
    public Posting() {
        root = new Text("");
        hyperlink = new Text("");
        title = new Text("");
        count = new IntWritable(0);
    }
    public Posting(Text r, Text h, Text t, IntWritable c) {
        root = r;
        hyperlink = h;
        title = t;
        count = c;
    }
    
    public Posting(String  r, String h, String t, Integer c) {
        root = new Text(r);
        hyperlink = new Text(h);
        title = new Text(t);
        count = new IntWritable(c);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        root.readFields(in);
        hyperlink.readFields(in);
        title.readFields(in);
        count.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        root.write(out);
        hyperlink.write(out);
        title.write(out);
        count.write(out);        
    }
    
    public Text getTitle() {
        return title;
    }
    
    public void setTitle(Text title) {
        this.title = title;
    }
    
    public Text getHyperlink() {
        return hyperlink;
    }
    
    public void setHyperlink(Text hyperlink) {
        this.hyperlink = hyperlink;
    }
    
    public IntWritable getCount() {
        return count;
    }
    
    public void setCount(IntWritable count) {
        this.count = count;
    }
    
    public Text getRoot() {
        return root;
    }
    
    public void setRoot(Text root) {
        this.root = root;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
        .append(root)
        .append(hyperlink)
        .append(title)
        .append(count)
        .toHashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Posting)) {
            return false;
        }
        
        Posting posting = (Posting) o;
        
        return new EqualsBuilder()
        .append(root, posting.root)
        .append(hyperlink, posting.hyperlink)
        .append(title, posting.title)
        .append(count , posting.count)
        .isEquals();
    }
    
    @Override
    public String toString() {
        return this.root.toString() + "|" + this.hyperlink.toString() + "|" + this.title.toString() + "|" + this.count.toString();
    }
    
    public static List<Posting> fromResult(String cf, String cq, Result result) {
        String json = Bytes.toString(result.getValue(Bytes.toBytes(cf), Bytes.toBytes(cq)));
        Gson gson = new Gson();
        List<Posting> postings = new ArrayList<Posting>();
        HashMap<String, HashMap<String, Integer>> m2;
        HashMap<String, Integer> m1;
        Type type = new TypeToken<HashMap<String, HashMap<String, HashMap<String, Integer>>>>(){}.getType();
        HashMap<String, HashMap<String, HashMap<String, Integer>>> map = gson.fromJson(json, type);
        for (String root : map.keySet()) {
            m2 = map.get(root);
            for (String title: m2.keySet()) {
                m1 = m2.get(title);
                for (String href : m1.keySet()) {
                    postings.add(new Posting(root, href, title, m1.get(href)));
                }
            }
        }
        return postings;
    }
    @Override
    public int compareTo(Posting o) {
        return this.getCount().compareTo(o.getCount());
    }
}
