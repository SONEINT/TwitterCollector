package edu.isi.twitter;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

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
		TwitterApplicationManager mgr = new TwitterApplicationManager();
		runTwitterStreamListenerThread(mgr);
		runUserProfileFillerThread(mgr);
//		runUserNetworkFetcherThread();
		runTwitterTimeLineFetcher(mgr);
	}
	
	private static void runTwitterStreamListenerThread(TwitterApplicationManager mgr) {
		logger.info("Starting Twitter stream listener thread");
		Thread t = new Thread(new TwitterUsersStreamDumper(mgr.getOneConfigurationBuilderByTag(ApplicationTag.Streaming)));
		t.start();
	}

	private static void runUserProfileFillerThread(TwitterApplicationManager mgr) {
		logger.info("Starting User profile lokup thread");
		Thread t = new Thread(new UserProfileFiller(mgr.getOneConfigurationBuilderByTag(ApplicationTag.UserProfileLookup)));
		t.start();
	}
	
	private static void runUserNetworkFetcherThread(TwitterApplicationManager mgr) {
		logger.info("Starting user network fetcher thread");
		Thread t = new Thread(new UsersFriendsAndFollowersManager(mgr.getOneConfigurationBuilderByTag(ApplicationTag.UserNetworkGraphFetcher)));
		t.start();
	}

	private static void runTwitterTimeLineFetcher(TwitterApplicationManager mgr) {
		logger.info("Starting user's timeline fetcher thread");
		List<Thread> allThreads = new ArrayList<Thread>();
		try {
			Mongo m = new Mongo("localhost", 27017 );
			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
			List<ConfigurationBuilder> allConfigs = mgr.getAllConfigurationBuildersByTag(ApplicationTag.UserTimelineFetcher);
			
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
		        allThreads.add(t);
		        t.start();
		        logger.info("Application # " + i);
		        logger.info("Start Index: " + startIndex);
		        logger.info("End Index: " + endIndex);
			}
			m.close();
			// wait for the all the threads to finish before starting again
			for (Thread t : allThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			logger.info("All timeline fetcher threads finished! Looping again ...");
			// Run the method again when all the threads have finished
			runTwitterTimeLineFetcher(mgr);
			
		} catch (UnknownHostException e) {
			logger.error("Error occured with Mongo host!", e);
		} catch (MongoException e) {
			logger.error("Mongo Exception!", e);
		}
		
	}
}