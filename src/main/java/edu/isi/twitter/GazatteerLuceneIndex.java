package edu.isi.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

public class GazatteerLuceneIndex {

	private File csvFile;
	private String[] fields;
	private boolean createNewIndex;
	public static final String INDEX_DIRECTORY = "lucene-index";
	public static final Version APP_LUCENE_VERSION = Version.LUCENE_36;
	private static Logger logger = LoggerFactory.getLogger(GazatteerLuceneIndex.class);

	public GazatteerLuceneIndex(File csvFile, boolean createNewIndex) {
		this.csvFile = csvFile;
		this.createNewIndex = createNewIndex;
	}
	
	public String[] createIndex() throws IOException {
		if (!csvFile.exists()) {
			throw new FileNotFoundException("CSV file not found: " + csvFile);
		}
		
		// Prepare the index writer of Lucene
		IndexWriterConfig config = getIndexWriterConfig();
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
	    					
	    					splitVal = splitVal + " " + timezone;
			    			d.add(new Field(headers.get(i), splitVal, Field.Store.YES, Field.Index.ANALYZED));
	    				}
	    			} else {
	    				s = s + " " + timezone;
		    			d.add(new Field(headers.get(i), s, Field.Store.YES, Field.Index.ANALYZED));
	    			}
	    		}
	    		System.out.println(d);
	    		indexWriter.addDocument(d);
	    	}
	    	if (lineIndex%100 == 0) {
	    		logger.info("Indexed " + lineIndex + " rows.");
	    	}
	    }
	    indexWriter.close();
	    reader.close();
	    logger.info("Done creating index.");
		return null;
	}
	
	private IndexWriterConfig getIndexWriterConfig() {
		IndexWriterConfig config = new IndexWriterConfig(GazatteerLuceneIndex.APP_LUCENE_VERSION
				, new StandardAnalyzer(GazatteerLuceneIndex.APP_LUCENE_VERSION));
		if (createNewIndex)
			config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		else
			config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
		return config;
	}
	
	public static void main(String[] args) {
		GazatteerLuceneIndex g = new GazatteerLuceneIndex(new File("data/test.csv"), true);
		try {
			g.createIndex();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
