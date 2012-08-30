package edu.isi.db;

import java.net.UnknownHostException;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBHandler {
	public static String HOSTNAME = "localhost";
	
	public static Mongo getNewMongoConnection() throws UnknownHostException, MongoException {
		return new Mongo(HOSTNAME, 27017 );
	}

}
