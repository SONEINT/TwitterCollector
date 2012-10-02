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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

public class LuceneIndexCreatorFromCSV {
	
	private File csvFile;
	private boolean createNewIndex;
	public static final String INDEX_DIRECTORY = "lucene-index";
	private static Logger logger = LoggerFactory.getLogger(LuceneIndexCreatorFromCSV.class);
	
	public LuceneIndexCreatorFromCSV(File csvFile, boolean createNewIndex) {
		this.csvFile = csvFile;
		this.createNewIndex = createNewIndex;
	}
	
	public void parseAndCreateIndex() throws IOException {
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
	    		for (int i=0; i<nextLine.length; i++) {
	    			
	    			String s = nextLine[i];
	    			// Skip empty values
	    			if(s.trim().equals(""))
	    				continue;
	    			d.add(new Field(headers.get(i), s, Field.Store.YES, Field.Index.ANALYZED));
	    		}
	    		indexWriter.addDocument(d);
	    	}
	    	if (lineIndex%100 == 0) {
	    		logger.info("Indexed " + lineIndex + " rows.");
	    	}
	    }
	    indexWriter.close();
	    logger.info("Done creating index.");
	}

	public static void main(String[] args) {
		LuceneIndexCreatorFromCSV crt = new LuceneIndexCreatorFromCSV(new File("seedData/test.csv"), true);
		try {
			crt.parseAndCreateIndex();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public IndexWriterConfig getIndexWriterConfig() {
		IndexWriterConfig config = new IndexWriterConfig(GazetteerLuceneManager.APP_LUCENE_VERSION
				, new StandardAnalyzer(GazetteerLuceneManager.APP_LUCENE_VERSION));
		if (createNewIndex)
			config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		else
			config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
		return config;
	}

}
