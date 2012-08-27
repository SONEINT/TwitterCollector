package edu.isi.twitter;

import java.net.UnknownHostException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;
import edu.isi.twitter.rest.UserProfileFiller;
import edu.isi.twitter.rest.UsersFriendsAndFollowersManager;
import edu.isi.twitter.rest.UsersHistoricalTweetsFetcherThread;
import edu.isi.twitter.streaming.TwitterUsersStreamDumper;

public class JobManager {
	
	private static Logger logger = LoggerFactory.getLogger(JobManager.class);
	
	public enum TwitterAccountKeys {
		user_id, access_token, access_token_secret, consumer_key, consumer_key_secret
	}
	
	public static void main(String[] args) {
		runTwitterStreamListenerThread();
		runUserProfileFillerThread();
		runUserNetworkFetcherThread();
		runTwitterTimeLineFetcher();
	}
	
	private static void runTwitterStreamListenerThread() {
		logger.info("Starting Twitter stream listener thread");
		Thread t = new Thread(new TwitterUsersStreamDumper(TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.Streaming)));
		t.run();
	}

	private static void runUserProfileFillerThread() {
		logger.info("Starting User profile lokup thread");
		Thread t = new Thread(new UserProfileFiller(TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.UserProfileLookup)));
		t.run();
	}
	
	private static void runUserNetworkFetcherThread() {
		logger.info("Starting user network fetcher thread");
		Thread t = new Thread(new UsersFriendsAndFollowersManager(TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.UserNetworkGraphFetcher)));
		t.run();
	}

	private static void runTwitterTimeLineFetcher() {
		logger.info("Starting user's timeline fetcher thread");
		try {
			Mongo m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
			List<ConfigurationBuilder> allConfigs = TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.UserTimelineFetcher);
			
			/** Calculating the number and range of users to be handled by each application thread **/
			DBCollection userColl = twitterDb.getCollection(TwitterCollections.users.name());
			long totallUsers = userColl.getCount();
			long interval = totallUsers/allConfigs.size();
			
			logger.info("Total users in user's table: " + totallUsers);
			logger.info("Interval chosen: " + interval);
			
			for(int i=0; i<allConfigs.size(); i++) {
				ConfigurationBuilder config = allConfigs.get(i);
				
				/*** Get the start index and end index for each application thread ***/
				int startIndex = new Long(i*interval).intValue();
		        int endIndex = (i != allConfigs.size()-1) ? new Long(i*interval + interval -1).intValue() : new Long(userColl.count()-1).intValue();
		        
		        Thread t = new Thread(new UsersHistoricalTweetsFetcherThread(startIndex, endIndex, config));
		        t.start();
		        logger.info("Application # " + i);
		        logger.info("Start Index: " + startIndex);
		        logger.info("End Index: " + endIndex);
			}
			
		} catch (UnknownHostException e) {
			logger.error("Error occured with Mongo host!", e);
		} catch (MongoException e) {
			logger.error("Mongo Exception!", e);
		}
		
	}

	/*
	private static long getUIDAtInterval(DBCollection coll, int offset) {
		System.out.println("Getting UID at interval: " + offset);
		long uid = 0L;
		DBCursor cursor = coll.find().skip(offset).limit(1);
        while(cursor.hasNext()) {
        	DBObject obj = cursor.next();
        	if(obj.containsField("uid")) {
        		uid = Long.parseLong(obj.get("uid").toString());
        	}
        }
        cursor.close();
        return uid;
	}
	*/
}