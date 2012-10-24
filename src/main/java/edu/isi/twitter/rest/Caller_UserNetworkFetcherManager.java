package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.twitter.TwitterApplicationManager;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;

public class Caller_UserNetworkFetcherManager {
	
	private static Logger logger = LoggerFactory.getLogger(UserNetworkFetcher.class);
	
	public static void main (String[] args) {
		clearOldThreadsFromTable();
		logger.info("Starting user's network fetcher manager...");
		List<Thread> allThreads = new ArrayList<Thread>();
		List<ConfigurationBuilder> allConfigs = TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.UserNetworkGraphFetcher, "twitter");
		
		// int minIteration = getMinimumIterationforTable(TwitterCollections.usersgraphlist);
				
		/* Starting a thread for each application */
		for(int i=0; i<allConfigs.size(); i++) {
			//ConfigurationBuilder config = allConfigs.get(i);
			// Thread t = new Thread(new UserNetworkFetcherThread(0, config, i));
	        // allThreads.add(t);
	        // t.start();
		}
		// wait for the all the threads to finish before starting again
		for (Thread t : allThreads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logger.info("All network fetcher threads finished!");
	}

//	private static int getMinimumIterationforTable(TwitterCollections collection) {
//		Mongo m = null;
//		try {
//			m = MongoDBHandler.getNewMongoConnection();
//			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
//			DBCollection coll = twitterDb.getCollection(collection.name());
//
//			DBCursor dbC = coll.find().sort(new BasicDBObject("iteration", 1)).limit(1);
//			if (dbC.count() != 0)
//				return Integer.parseInt(dbC.next().get("iteration").toString());
//			else {
//				logger.error("Something wrong in the query or table. No iteration found in " + collection.name());
//				return 0;
//			}
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		} catch (MongoException e) {
//			e.printStackTrace();
//		} finally {
//			m.close();
//		}
//		return 0;
//	}

	private static void clearOldThreadsFromTable() {
		logger.info("Removing all existing rows from the currentThreads table...");
		Mongo m;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB("twitter");
			DBCollection currentThreadsColl = twitterDb.getCollection(TwitterCollections.currentThreads.name());

			// Clear all the existing rows
			currentThreadsColl.remove(new BasicDBObject());
			m.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

}
