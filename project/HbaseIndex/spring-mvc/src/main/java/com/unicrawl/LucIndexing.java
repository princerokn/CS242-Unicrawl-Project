package com.unicrawl;

import com.unicrawl.EducationalDataCrawling;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class LucIndexing {

    static final String FILES_TO_INDEX_DIRECTORY = "docsPath//";
    static final String INDEX_DIRECTORY = EducationalDataCrawling.index + "//";

    private IndexWriter createWriter() throws IOException {
        FSDirectory dir = FSDirectory.open(Paths.get(INDEX_DIRECTORY));
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter writer = new IndexWriter(dir, config);
        return writer;
    }

    private List<Document> createDocs() throws IOException {
        List<Document> docs = new ArrayList<>();
        FieldType bodyType = new FieldType();
        bodyType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        bodyType.setStored(true);
        bodyType.setTokenized(true);
        FieldType urlType = new FieldType();
        urlType.setIndexOptions(IndexOptions.DOCS);
        urlType.setStored(true);
        urlType.setOmitNorms(true);
        urlType.setTokenized(true);
        urlType.setStoreTermVectors(true);
        FieldType titleType = new FieldType();
        titleType.setIndexOptions(IndexOptions.DOCS);
        titleType.setStored(true);
        titleType.setTokenized(true);
        bodyType.setStoreTermVectors(true);
        bodyType.setStoreTermVectorPositions(true);
        bodyType.setStoreTermVectorOffsets(true);
        bodyType.setStoreTermVectorPayloads(true);
        String url, title, body, alexa;
        File dir = new File(FILES_TO_INDEX_DIRECTORY);
        File[] files = dir.listFiles();
        for (File file : files) {
            System.out.println("Indexing: " + file.getName());
            org.jsoup.nodes.Document openDoc = Jsoup.parse(file, "UTF-8", "http://example.com/");

            Element urlElement = openDoc.select("url").first();
            if (urlElement == null)
                continue;
            Element bodyElement = openDoc.select("doc").first();
            Element titleElement = openDoc.select("title").first();

            url = urlElement.text();
            body = bodyElement.text();
            title = titleElement.text();

            Document doc = new Document();
            Field titleField = new Field("titletext", title, titleType);
            doc.add(titleField);
            doc.add(new Field("bodytext", body, bodyType));
            doc.add(new Field("urltext", url, urlType));
            docs.add(doc);
        }

        return docs;
    }

    private IndexSearcher createSearcher() throws IOException {
        Directory dir = FSDirectory.open(Paths.get(INDEX_DIRECTORY));
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        return searcher;
    }

    public void indexing() throws ParseException, IOException {
        // indexing
        IndexWriter writer = createWriter();
        writer.deleteAll();
        List<Document> docs = createDocs();
        writer.addDocuments(docs);
        writer.commit();
        writer.close();

    }

    public List<Output> search(String search) throws IOException, ParseException {
        // search
        List<Output> results = new ArrayList<>();
        IndexSearcher searcher = createSearcher();
        QueryParser qp = new QueryParser("bodytext", new StandardAnalyzer());
        Query q1 = qp.parse(search);
        TopDocs hits = searcher.search(q1, 20);
        System.out.println(hits.totalHits + " docs found for the query \"" + q1.toString() + "\"");
        int num = 0;
        Queue<String> q = new LinkedList<>();
        for (ScoreDoc sd : hits.scoreDocs) {
            Document d = searcher.doc(sd.doc);
            String x = d.get("urltext");
            float score = sd.score;
            int length = x.length();
            if (x.charAt(length - 1) == '/') {
                x = x.substring(0, length - 1);
            }
            if (q.contains(x))
                continue;
            System.out.println(String.format("#%d: %s (url=%s) %f", ++num, d.get("titletext"), x, score));
            q.add(x);
            results.add(new Output(num, d.get("titletext"),x,score));
            if (num == 10)
                break;
        }
        return results;
    }
}


