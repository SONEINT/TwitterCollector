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
			IndexReader rdr = IndexReader.open(FSDirectory.open(new File(GazatteerLuceneIndex.INDEX_DIRECTORY)));
		    IndexSearcher is = new IndexSearcher(rdr);

		    QueryParser parser = new QueryParser(GazatteerLuceneIndex.APP_LUCENE_VERSION, "alternative names", new StandardAnalyzer(GazatteerLuceneIndex.APP_LUCENE_VERSION));
		    Query query = parser.parse("Zābol~ Kabol~");
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
		        System.out.println((i + 1) + ". " + d.get("name"));
		    }
		    is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
  }
}