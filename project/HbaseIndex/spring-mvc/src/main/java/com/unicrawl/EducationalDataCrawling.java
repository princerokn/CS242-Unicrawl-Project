package com.unicrawl;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

public class EducationalDataCrawling {
    static ArrayList<String> pagesAlreadyVisited = new ArrayList<>();
    static Queue<String> masterQueue = new LinkedList<>();
    static Scanner sc = new Scanner(System.in);
    static String path;
    static String index;
    static int subLinks;
    Queue<String> q = new LinkedList<>();
    static void prepareFilesandLinks() throws IOException {
        System.out.println("Enter the location where you want to store the Crawled files ");
        path = sc.next();
        System.out.println("Enter the location where you want to store the Indexed files ");
        index = sc.next();
        System.out.println("Enter the number of subLinks you want to visit for each university ");
        subLinks = sc.nextInt();
        File myOb = new File(path + "\\Links.txt");
        myOb.createNewFile();
        String url = "http://doors.stanford.edu/~sr/universities.html";
        Document document1 = Jsoup.connect(url).ignoreContentType(true).timeout(0).ignoreHttpErrors(true).get();
        Elements links1 = document1.select("a[href]");
        for (Element link : links1) {
            String collegeUrl = link.attr("href");
            if (collegeUrl.endsWith(".edu/")) {
                masterQueue.add(collegeUrl);
            }
        }
        pagesAlreadyVisited.add(url);
    }

    void crawl() throws Exception {
        //using proxy so as to stay anonymous
        System.setProperty("http.proxyhost", "127.0.0.1");
        System.setProperty("http.proxyport", "3128");
        SslUtils.ignoreSsl();
        try {
            Whitelist wl = new Whitelist();
            Cleaner x = new Cleaner(wl);
            q.clear();
            pagesAlreadyVisited.clear();
            if (masterQueue.isEmpty())
                return;
            String url = masterQueue.poll();
            URL name = new URL(url);
            String university = name.getHost();
            outerloop:
            for (int i = 0; i < subLinks; i++) {
                Document document = Jsoup.connect(url).ignoreContentType(true).timeout(10000).ignoreHttpErrors(true).get(); //get html page
                x.clean(document);
                //get all the link in the page
                Elements links = document.select("a[href]");
                for (Element link : links) {
                    q.add(link.attr("href"));
                }

                InetAddress address = InetAddress.getByName(new URL(url).getHost());
                String ip = address.getHostAddress();

                String filename = university.substring(4) + i + ".html";
                String htmlContent = document.body().html();
                String content = "<doc>\n" +
                        "<filename>"
                        + filename
                        + "</filename>\n"
                        + "<url>"
                        + url + "</url>\n"
                        + "<ip>"
                        + ip + "</ip>\n"
                        + "<title>"
                        + document.title()
                        + "</title>\n"
                        + "<body>\n"
                        + htmlContent
                        + "\n</body>\n"
                        + "</doc>";


                File myObj = new File(path + "\\" + filename);
                myObj.createNewFile();
                System.out.println("File created: " + myObj.getName());
                FileWriter myWriter = new FileWriter(path + "\\" + filename);
                myWriter.write(content);
                myWriter.close();

                FileWriter myWriter1 = new FileWriter(path + "\\Links.txt", true);
                myWriter1.write(url + "\n");
                myWriter1.close();
                while (true) {
                    if (q.isEmpty())
                        break outerloop;
                    url = q.poll();
                    if (url.length() > 10 && url.startsWith("http:")) {
                        if (!pagesAlreadyVisited.contains(url)) {
                            pagesAlreadyVisited.add(url);
                            break;
                        }
                    }
                }
                System.out.println("URL Visited" + url);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

