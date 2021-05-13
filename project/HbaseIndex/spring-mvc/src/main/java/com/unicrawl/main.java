package com.unicrawl;

import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.Scanner;

public class main {

    public static void main(String[] args) throws IOException, ParseException {
        Scanner sc = new Scanner(System.in);
        String userquery = sc.next();
        LucIndexing lin = new LucIndexing();
        lin.indexing();
        lin.search(userquery);
    }
}