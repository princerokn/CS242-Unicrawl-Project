package com.unicrawl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unicrawl.Output;

public class Search {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private String query;
    private String jar;

    public Search(String query, String jar) {
        this.query = query;
        this.jar = jar;
    }

    public List<Output> run() throws IOException, InterruptedException {
        int index = 1;
        Output result;
        String tmp = "/tmp/rank-" + System.currentTimeMillis();
        List<Output> results = new ArrayList<>();
        Runtime rt = Runtime.getRuntime();
        String command = "hadoop jar " + jar + " edu.ucr.abhi.search.Rank " + tmp + " " + query;
        System.out.println("Executing command :" + command);
        Process pr = rt.exec(command);
        if (pr.waitFor() == 0) {
            command = "hadoop fs -cat " + tmp + "/*";
            System.out.println("Executing command :" + command);
            pr = rt.exec(command);
            if (pr.waitFor() == 0) {
                BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                String line = null;
                while ((line = input.readLine()) != null) {
                    System.out.println("#" + index + " -- " + line);
                    result = objectMapper.readValue(line, Output.class);
                    result.setNum(index);
                    results.add(result);
                    index++;
                }
                command = "hadoop fs -rm -r -skipTrash " + tmp;
                System.out.println("Executing command :" + command);
                pr = rt.exec(command);
                pr.waitFor();
            }
        }
        return results;
    }

}
