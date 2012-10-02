package edu.isi.twitter;
import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.FSDirectory;

public class LuceneIndexQueryTester {
	public static void main(String[] args) throws IOException, ParseException {
		try {
//			GazetteerLuceneManager gzMgr = new GazetteerLuceneManager();
//			try {
//			//				gzMgr.createIndexFromGazetteerCSV(new File("data/test.csv"), true);
//				gzMgr.createIndexFromGazetteerCSV(new File("data/test.csv"), true);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
		  
			IndexReader rdr = IndexReader.open(FSDirectory.open(new File(GazetteerLuceneManager.INDEX_DIRECTORY)));
		    IndexSearcher is = new IndexSearcher(rdr);
		
		    QueryParser parser = new QueryParser(GazetteerLuceneManager.APP_LUCENE_VERSION, "original name", new StandardAnalyzer(GazetteerLuceneManager.APP_LUCENE_VERSION));
			String test = "I\\n+y-o!u(r)X:b^o]x{3}6~0*P?a]c[i||fic, OR~ + Time \\(US & Canada\\)~";
//		    System.out.println(test.replaceAll("[\\\\\\+-!\\(\\):^]{}~*?]", " "));
			
//			System.out.println(test.replaceAll("[\\\\\\+\\-\\!\\(\\):^\\]\\[{}~*?]", " "));
//			System.out.println(test.replaceAll("OR", " "));
//			System.out.println(test.replaceAll("\\|\\|", " "));
			String test2 = "NEW Delhi";
			System.out.println(test2.replaceAll("(?i)new", ""));
		    
		    
		    Query query = parser.parse("New asdadsDelhi~0.9");
			//		    Query query = new FuzzyQuery(new Term("name", "Kunduz"));
			
			//		    PhraseQuery query = new PhraseQuery();
			//		    query.add(new Term("name", "Kunduz Kabul"));
			//		    query.add(new Term("name", "Kabul"));
			
			
			TopScoreDocCollector collector = TopScoreDocCollector.create(10, true);
			is.search(query, collector);
			ScoreDoc[] hits = collector.topDocs().scoreDocs;
			
			System.out.println("Found " + hits.length + " hits.");
			for(int i=0;i<hits.length;++i) {
			    int docId = hits[i].doc;
			    System.out.println("Score: "+ hits[i].score);
			    Document d = is.doc(docId);
			    System.out.println(d);
//			    System.out.println((i + 1) + ". " + d.get("name"));
			}
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}