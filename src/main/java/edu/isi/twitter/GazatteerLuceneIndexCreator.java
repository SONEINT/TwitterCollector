package edu.isi.twitter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import au.com.bytecode.opencsv.CSVReader;

public class GazatteerLuceneIndexCreator {

	public static void main(String[] args) {
		CSVReader reader;
		try {
			reader = new CSVReader(new InputStreamReader(new FileInputStream("seedData/middle-east-gazatteer.csv"), "UTF-8"));
			String [] nextLine;
		    while ((nextLine = reader.readNext()) != null) {
		        System.out.println(nextLine[0] + nextLine[1] + "etc...");
		    }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	}

}
