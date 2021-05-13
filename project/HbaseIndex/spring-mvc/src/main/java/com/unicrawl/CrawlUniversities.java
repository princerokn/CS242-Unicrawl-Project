package com.unicrawl;

import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.Test;
import org.junit.runner.Result;

import java.io.IOException;
import java.util.Scanner;

public class CrawlUniversities {
    static int I = 0;
    EducationalDataCrawling m = new EducationalDataCrawling();

    public static void main(String[] args) throws IOException, ParseException {
        EducationalDataCrawling.prepareFilesandLinks();
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter the number of Universities you want to crawl ");
        int n = sc.nextInt();
        if (I > 1015) {
            n = 1015;
            System.out.println("\n Sorry you can crawl upto 1015 universities only");
        }
        boolean runForever = true;
        while (runForever) {
            Result result = org.junit.runner.JUnitCore.runClasses(CrawlUniversities.class);
            if (I > n) {
                runForever = false;
            }
        }
        LucIndexing lin = new LucIndexing();
        lin.indexing();
    }

    @Test
    public void test() {
        I++;
        try {
            m.crawl();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(I);
    }

}