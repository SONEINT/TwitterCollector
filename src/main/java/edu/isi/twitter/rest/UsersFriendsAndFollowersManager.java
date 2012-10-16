package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

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

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;

public class UsersFriendsAndFollowersManager implements Runnable {

	// private ConfigurationBuilder cb;
	
	private Logger logger = LoggerFactory.getLogger(UsersFriendsAndFollowersManager.class);
	
//	public UsersFriendsAndFollowersManager(ConfigurationBuilder cb) {
//		this.cb = cb;
//	}
	
	public void run() {
		try {
			Mongo m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
			DB twitterDb = m.getDB("twitter");
			DBCollection usersColl = twitterDb.getCollection(TwitterCollections.users.name());
//			DBCollection usersGraphListColl = twitterDb.getCollection("usersGraphListTest");
//			DBCollection usersGraphColl = twitterDb.getCollection(TwitterCollections.usersGraph.name());
//			DBCollection usersGraphActionListColl = twitterDb.getCollection(TwitterCollections.usersGraphActionList.name());
//			Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
			
			DBObject query = new BasicDBObject("onceDone", new BasicDBObject("$exists", false));
			
			while (true) {
				DBCursor cursor = usersColl.find(query);
				while (cursor.hasNext()) {
					DBObject user = cursor.next();
					if(!user.containsField("uid")) {
						logger.error("No uid found for " + user);
						continue;
					}
					Double d = Double.parseDouble(user.get("uid").toString());
	            	long uid = d.longValue();
	            	logger.info("Getting network for the user: " + uid);
//					UserNetworkFetcher f = new UserNetworkFetcher(uid);
//					boolean success = f.fetchAndStoreInDB(usersGraphColl, usersGraphActionListColl, authenticatedTwitter);
//					if (success) {
//						user.put("onceDone", true);
//						usersGraphListColl.save(user);
//					} else {
//						user.put("problemOccured", true);
//						usersGraphListColl.save(user);
//					}
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
//		UsersFriendsAndFollowersManager mgr = new UsersFriendsAndFollowersManager(TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.UserNetworkGraphFetcher));
//		mgr.run();
	}
}
