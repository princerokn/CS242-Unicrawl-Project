package com.unicrawl.controller;

import com.unicrawl.LucIndexing;
import com.unicrawl.Output;
import com.unicrawl.Search;

import org.apache.lucene.queryparser.classic.ParseException;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Controller
public class HomeController {
    List<Output> lucene = new ArrayList<>();
    List<Output> hadoop = new ArrayList<>();
    @GetMapping("/home")
    public String home(@RequestParam(required = false) String query, Model model) throws IOException, ParseException, InterruptedException {
        System.out.println("Query :" + query);
        LucIndexing lin = new LucIndexing();
        lucene = lin.search(query);
        Search search = new Search(query, "/home/karna/projects/HbaseIndex/searchmr/target/HbaseIndex-1.0.0-jar-with-dependencies.jar");
        hadoop = search.run();
	System.out.println("Hadoop results size :" + hadoop.size());
        model.addAttribute("query", query);
        return "redirect:/results";
    }
    @GetMapping("/results")
    public String results(Model model){
        model.addAttribute("lucene", lucene);
        model.addAttribute("hadoop", hadoop);
        return "query_results";
    }
}