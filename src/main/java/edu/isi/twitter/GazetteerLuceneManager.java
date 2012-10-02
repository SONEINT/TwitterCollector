package edu.isi.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

public class GazetteerLuceneManager {

	private Set<String> indexedFields = new HashSet<String>();
	private IndexSearcher indexSearcher;
	
	// private static float LUCENE_THRESHOLD_SCORE = 1.5f; 
	public static final String INDEX_DIRECTORY = "lucene-index";
	public static final Version APP_LUCENE_VERSION = Version.LUCENE_36;
	
	private static Logger logger = LoggerFactory.getLogger(GazetteerLuceneManager.class);
	
	public GazetteerLuceneManager() {
		try {
			IndexReader indexReader = IndexReader.open(FSDirectory.open(new File(INDEX_DIRECTORY)));
			indexSearcher = new IndexSearcher(indexReader);
		} catch (IOException e) {
			logger.error("Error occured while attempting to setup index reader.", e);
		}
	}

	public void createIndexFromGazetteerCSV(File csvFile, boolean createNewIndex) throws IOException {
		if (!csvFile.exists()) {
			throw new FileNotFoundException("CSV file not found: " + csvFile);
		}
		
		// Prepare the index writer of Lucene
		IndexWriterConfig config = getIndexWriterConfig(createNewIndex);
		IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(INDEX_DIRECTORY)), config);
		
		// Start reading the CSV file
		CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(csvFile), "UTF-8"));
		String [] nextLine;
		int lineIndex = 0;
		List<String> headers = new ArrayList<String>();
	    while ((nextLine = reader.readNext()) != null) {
	    	lineIndex++;
	    	// Check for the headers in the first line
	    	if (lineIndex == 1) {
	    		for (String s: nextLine)
	    			headers.add(s);
	    	}
	    	
	    	// Populate the index with data
	    	else {
	    		// Skip the lines where the # values != number of header names
	    		if (nextLine.length != headers.size())
	    			continue;
	    		Document d = new Document();
	    		// Assuming that timezone is always in the last column
	    		String timezone = nextLine[nextLine.length-1];
	    		// Loop through each value except the last one
	    		for (int i=0; i<nextLine.length-1; i++) {
	    			String s = nextLine[i];
	    			// Skip empty values
	    			if(s.trim().equals(""))
	    				continue;
	    			
	    			// Check if the value needs to be split by comma (such as in cases where value contains alternate names)
	    			if (s.contains(",")) {
	    				String[] values = s.split(",");
	    				for (int j=0; j<values.length; j++) {
	    					String splitVal = values[j];
	    					if (splitVal.trim().equals("")) continue;
	    					// Adding the timezone to the value
	    					splitVal = splitVal + " " + timezone;
			    			d.add(new Field(headers.get(i), splitVal, Field.Store.YES, Field.Index.ANALYZED));
	    				}
	    			} else {
	    				s = s + " " + timezone;
		    			d.add(new Field(headers.get(i), s, Field.Store.YES, Field.Index.ANALYZED));
	    			}
	    		}
	    		indexWriter.addDocument(d);
	    	}
	    	if (lineIndex%2000 == 0) {
	    		logger.info("Indexed " + lineIndex + " rows.");
	    	}
	    }
	    indexWriter.close();
	    reader.close();
	    logger.info("Done creating index.");
	    
	    // Setup the index reader
	    IndexReader indexReader = IndexReader.open(FSDirectory.open(new File(INDEX_DIRECTORY)));
		indexSearcher = new IndexSearcher(indexReader);
		
		indexedFields.addAll(headers);
	}
	
	public boolean isLocationMatchingToGazetteerFeature(String location, String timezone) throws ParseException, IOException {
		for (String field : indexedFields) {
			QueryParser parser = new QueryParser(GazetteerLuceneManager.APP_LUCENE_VERSION, field, new StandardAnalyzer(GazetteerLuceneManager.APP_LUCENE_VERSION));
			String locationNormalized = normalizeForQueryParser(location);
			String timezoneNormalized = normalizeForQueryParser(timezone);
			
			if (locationNormalized.trim().equals("") && timezoneNormalized.trim().equals(""))
				return false;
			
			Query query = null;
			if (timezoneNormalized.trim().equals("") && !locationNormalized.trim().equals(""))
				query = parser.parse(locationNormalized + "~0.9");
			else if (locationNormalized.trim().equals("") && !timezoneNormalized.trim().equals(""))
				query = parser.parse(timezoneNormalized + "~0.9");
			else
				query = parser.parse(locationNormalized + "~0.9 " + timezoneNormalized + "~0.9");
		    
		    TopScoreDocCollector collector = TopScoreDocCollector.create(10, true);
		    indexSearcher.search(query, collector);
		    
		    ScoreDoc[] hits = collector.topDocs().scoreDocs;
		    
		    if (hits.length == 0)
		    	continue;
		    
		    if(hits[0].score > 1.5f) { // TODO: Change to LUCENE_THRESHOLD_SCORE later
		    	// int docId = hits[0].doc;
		        // Document d = indexSearcher.doc(docId);
		    	// System.out.println("Found match from Lucene for LC:" + locationNormalized + " TZ:" + timezoneNormalized  + "! Document matching field: " + d.get(field));
		    	return true;
		    }
//		    	
//		    System.out.println("Found " + hits.length + " hits.");
//		    for(int i=0;i<hits.length;++i) {
//		        int docId = hits[i].doc;
//		        System.out.println("Score: "+ hits[i].score);
//		        Document d = indexSearcher.doc(docId);
//		        System.out.println((i + 1) + ". " + d.get("name"));
//		    }
		}
		return false;
	}
	
	
	private String normalizeForQueryParser(String str) {
		str = str.replaceAll("[\\\\\\+\\-\\!\\(\\):^\\]\\[{}~*?]", " ");
		str = str.replaceAll("OR", "");
		str = str.replaceAll("\\|\\|", "");
		// Getting rid of common words that appear in location names
		str = str.replaceAll("(?i)new", "");
		str = str.replaceAll("(?i)city", "");
		str = str.replaceAll("(?i)north", "");
		str = str.replaceAll("(?i)south", "");
		str = str.replaceAll("(?i)east", "");
		str = str.replaceAll("(?i)central", "");
		str = str.replaceAll("(?i)west", "");
		str = str.replaceAll("(?i)river", "");
		str = str.replaceAll("(?i)sea", "");
		str = str.replaceAll("(?i)bay", "");
		str = str.replaceAll("AZ", "");
		str = str.replaceAll("WA", "");
		str = str.replaceAll("CA", "");
		return str;
	}

	private IndexWriterConfig getIndexWriterConfig(boolean createNewIndex) {
		IndexWriterConfig config = new IndexWriterConfig(GazetteerLuceneManager.APP_LUCENE_VERSION
				, new StandardAnalyzer(GazetteerLuceneManager.APP_LUCENE_VERSION));
		if (createNewIndex)
			config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		else
			config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
		return config;
	}
}
