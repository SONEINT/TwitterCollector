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

import edu.isi.filter.GazetteerManager;

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
		  
			IndexReader rdr = IndexReader.open(FSDirectory.open(new File(GazetteerManager.INDEX_DIRECTORY_PREFIX)));
		    IndexSearcher is = new IndexSearcher(rdr);
		
		    QueryParser parser = new QueryParser(GazetteerManager.APP_LUCENE_VERSION, "name", new StandardAnalyzer(GazetteerManager.APP_LUCENE_VERSION));
//			String test = "I\\n+y-o!u(r)X:b^o]x{3}6~0*P?a]c[i||fic, OR~ + Time \\(US & Canada\\)~";
//		    System.out.println(test.replaceAll("[\\\\\\+-!\\(\\):^]{}~*?]", " "));
			
//			System.out.println(test.replaceAll("[\\\\\\+\\-\\!\\(\\):^\\]\\[{}~*?]", " "));
//			System.out.println(test.replaceAll("OR", " "));
//			System.out.println(test.replaceAll("\\|\\|", " "));
//			String test2 = "NEW Delhi";
//			System.out.println(test2.replaceAll("(?i)new", ""));
		    
		    
		    Query query = parser.parse("Tel Aviv...~0.9 jerusalem~0.9");
			//		    Query query = new FuzzyQuery(new Term("name", "Kunduz"));
			
			//		    PhraseQuery query = new PhraseQuery();
			//		    query.add(new Term("name", "Kunduz Kabul"));
			//		    query.add(new Term("name", "Kabul"));
			
		    // Add Isreal term for testing
		 // Prepare the index writer of Lucene
//			IndexWriterConfig config = getIndexWriterConfig(false);
//			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(GazetteerLuceneManager.INDEX_DIRECTORY)), config);
//		    Document d2 = new Document();
//		    d2.add(new Field("country", "Israel", Field.Store.YES, Field.Index.ANALYZED));
//		    indexWriter.addDocument(d2);
//		    indexWriter.close();
		    
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
	
//	private static IndexWriterConfig getIndexWriterConfig(boolean createNewIndex) {
//		IndexWriterConfig config = new IndexWriterConfig(GazetteerManager.APP_LUCENE_VERSION
//				, new StandardAnalyzer(GazetteerManager.APP_LUCENE_VERSION));
//		if (createNewIndex)
//			config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
//		else
//			config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
//		return config;
//	}
}