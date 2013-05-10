package edu.isi.db;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class MongoDBHandler {
	public static String HOSTNAME = "localhost";
	
	public static Mongo getNewMongoConnection() throws UnknownHostException, MongoException {
		return new MongoClient(new ServerAddress(HOSTNAME), new MongoClientOptions.Builder().
				alwaysUseMBeans(true).
				autoConnectRetry(true).
				writeConcern(WriteConcern.NORMAL).
				connectTimeout(new Long(TimeUnit.SECONDS.toMillis(30)).intValue()).
				build());
	}
}