package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.twitter.TwitterApplicationManager;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;

public class UsersFriendsAndFollowersManager implements Runnable {

	ConfigurationBuilder cb;
	
	private Logger logger = LoggerFactory.getLogger(UsersFriendsAndFollowersManager.class);
	
	public UsersFriendsAndFollowersManager(ConfigurationBuilder cb) {
		this.cb = cb;
	}
	
	public void run() {
		try {
			Mongo m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
			DBCollection usersGraphListColl = twitterDb.getCollection(TwitterCollections.usersgraphlist.name());
//			DBCollection usersGraphListColl = twitterDb.getCollection("usersGraphListTest");
			DBCollection usersGraphColl = twitterDb.getCollection(TwitterCollections.usersGraph.name());
			DBCollection usersGraphActionListColl = twitterDb.getCollection(TwitterCollections.usersGraphActionList.name());
			Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
			
			while (true) {
				DBCursor cursor = usersGraphListColl.find();
				while (cursor.hasNext()) {
					DBObject user = cursor.next();
					if(!user.containsField("uid")) {
						logger.error("No uid found for " + user);
						continue;
					}
					Double d = Double.parseDouble(user.get("uid").toString());
	            	long uid = d.longValue();
	            	logger.info("Getting network for the user: " + uid);
					UserNetworkFetcher f = new UserNetworkFetcher(uid);
					boolean success = f.fetchAndStoreInDB(usersGraphColl, usersGraphActionListColl, authenticatedTwitter);
					if (success) {
						user.put("onceDone", true);
						usersGraphListColl.save(user);
					}
				}
				
				// Making the thread sleep for some time before trying again
				try {
					logger.info("Making the network fetcher thread sleep before starting the loop again!");
					Thread.sleep(TimeUnit.SECONDS.toMillis(30L));
					logger.info("Waking up the network watcher thread!");
				} catch (InterruptedException e) {
					logger.error("InterruptedException", e);
				}
			}
		} catch (UnknownHostException e) {
			logger.error("UnknownHostException", e);
		} catch (MongoException e) {
			logger.error("MongoException", e);
		}
		
	}
	
	public static void main(String[] args) {
		TwitterApplicationManager amgr = new TwitterApplicationManager();
		UsersFriendsAndFollowersManager mgr = new UsersFriendsAndFollowersManager(amgr.getOneConfigurationBuilderByTag(ApplicationTag.UserNetworkGraphFetcher));
		mgr.run();
	}
}
