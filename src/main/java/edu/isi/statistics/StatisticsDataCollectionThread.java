package edu.isi.statistics;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.linksStats_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.tweetsStats_SCHEMA;
import edu.isi.twitter.AppConfig;

public class StatisticsDataCollectionThread implements Runnable {

	private AppConfig appConfig;
	private String threadName = "StatisticsCollection";
	private static Logger logger = LoggerFactory.getLogger(StatisticsDataCollectionThread.class);
	
	public StatisticsDataCollectionThread(AppConfig config) {
		this.appConfig = config;
	}
	
	
	@Override
	public void run() {
		Thread.currentThread().setName(threadName);
		logger.info("Starting statistics collection thread: " + threadName);
		
		// Setup mongodb
		Mongo m = null;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
		} catch (UnknownHostException e) {
			logger.error("UnknownHostException", e);
		} catch (MongoException e) {
			logger.error("MongoException", e);
		}
		if(m == null) {
			logger.error("Error getting connection to MongoDB! Cannot proceed with this thread.");
			return;
		}
		
		DB twitterDb = m.getDB(appConfig.getDBName());
		DBCollection tweetStatsColl = twitterDb.getCollection(TwitterCollections.tweetsStats.name());
		DBCollection linkStatsColl = twitterDb.getCollection(TwitterCollections.linksStats.name());
		
		while (true) {
			DateTime now = new DateTime();
			// Get the tweets count and save it
			long tweetsCount = twitterDb.getCollection(TwitterCollections.tweets.name()).count();
			DBObject tweetStatsObj = new BasicDBObject(tweetsStats_SCHEMA.time.name(), now.getMillis())
				.append(tweetsStats_SCHEMA.timeReadable.name(), now.toString())
				.append(tweetsStats_SCHEMA.count.name(), tweetsCount);
			tweetStatsColl.insert(tweetStatsObj);
			
			// Get the links count and save it
			long linksCount = twitterDb.getCollection(TwitterCollections.usersGraph.name()).count();
			DBObject linksStatsObj = new BasicDBObject(linksStats_SCHEMA.time.name(), now.getMillis())
				.append(linksStats_SCHEMA.timeReadable.name(), now.toString())
				.append(linksStats_SCHEMA.count.name(), linksCount);
			linkStatsColl.insert(linksStatsObj);
			
			// Sleep the thread for 15 minutes and wake up again
			try {
				TimeUnit.MINUTES.sleep(1l);
			} catch (InterruptedException e) {
				logger.error("Error while sleeping thread of statistics collection!", e);
			}
		}	
	}
}
