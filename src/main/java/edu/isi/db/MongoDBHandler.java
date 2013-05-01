package edu.isi.db;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;

public class MongoDBHandler {
	public static String HOSTNAME = "localhost";
	
	public static Mongo getNewMongoConnection() throws UnknownHostException, MongoException {
		MongoOptions opt = new MongoOptions();
		opt.setAutoConnectRetry(true);
		opt.setAlwaysUseMBeans(true);
		int connectTimeoutDur = new Long(TimeUnit.SECONDS.toMillis(30)).intValue();
		opt.setConnectTimeout(connectTimeoutDur);
		return new Mongo(HOSTNAME, opt);
	}
}
