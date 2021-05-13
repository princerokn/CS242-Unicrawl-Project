package edu.ucr.abhi.search;

import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;


public class Query {

    public static Scan getScan() {
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        return scan;
    }

    public static boolean match(String rowKey, String query) {
        return query.contains(rowKey);
    }
    public static boolean match(String rowKey, Set<String> query) {
        return query.contains(rowKey);
    }
}