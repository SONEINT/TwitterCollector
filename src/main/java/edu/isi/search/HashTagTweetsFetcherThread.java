package edu.isi.search;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.seedHashTags_SCHEMA;
import edu.isi.search.SearchAPITweetsFetcher.QUERY_TYPE;
import edu.isi.twitter.AppConfig;

public class HashTagTweetsFetcherThread implements Runnable {
	
	private ConfigurationBuilder cb;
	private String threadName;
	private Logger logger = LoggerFactory.getLogger(HashTagTweetsFetcherThread.class);
	private AppConfig appConfig;

	public HashTagTweetsFetcherThread(ConfigurationBuilder cb, int index, AppConfig appConfig) {
		this.cb = cb;
		this.appConfig = appConfig;
		this.threadName = "HashTagTweetFetcher" + index;
	}

	@Override
	public void run() {
		Thread.currentThread().setName(threadName);
		logger.info("Starting hash tag tweet fetcher thread: " + threadName);
		
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
		DBCollection tweetsColl = twitterDb.getCollection(TwitterCollections.tweets.name());
		DBCollection currentThreadsColl = twitterDb.getCollection(TwitterCollections.currentThreads.name());
		DBCollection seedHashTagsColl = twitterDb.getCollection(TwitterCollections.seedHashTags.name());
		Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
		
		// Register the thread in the currentThreads table
		DBObject threadObj = new BasicDBObject("type", "TweetFetcher").append("name", threadName);
		currentThreadsColl.save(threadObj);
		
		while (true) {
			DBCursor crsr = seedHashTagsColl.find().snapshot();
			while (crsr.hasNext()) {
				DBObject seedHashtagObj = crsr.next();
				String hashTagVal = seedHashtagObj.get(seedHashTags_SCHEMA.value.name()).toString();
				logger.info("Getting tweets for hashtag: " + hashTagVal);
				
				int currentIteration = seedHashtagObj.containsField(seedHashTags_SCHEMA.iteration.name()) ? 
						Integer.parseInt(seedHashtagObj.get(seedHashTags_SCHEMA.iteration.name()).toString())+1 : 1;
				seedHashtagObj.put(seedHashTags_SCHEMA.iteration.name(), currentIteration);
				seedHashTagsColl.save(seedHashtagObj);
				
				// Get the tweets
				SearchAPITweetsFetcher tweetFetcher = new SearchAPITweetsFetcher(hashTagVal, authenticatedTwitter, QUERY_TYPE.hashTag);
				tweetFetcher.fetchAndStoreInDB(tweetsColl, currentThreadsColl, threadObj);
				
			}
		}
	}
	
}
