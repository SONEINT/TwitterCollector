package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.THREAD_TYPE;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.currentThreads_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.users_SCHEMA;
import edu.isi.statistics.StatisticsManager;
import edu.isi.twitter.AppConfig;

public class UserNetworkFetcherThread implements Runnable {
	
	private ConfigurationBuilder cb;
	private String threadName;
	private Logger logger = LoggerFactory.getLogger(UserNetworkFetcherThread.class);
	private static int USER_LIST_COUNT = 50;
	private AppConfig appConfig;
	private StatisticsManager statsMgr;
	
	public UserNetworkFetcherThread(ConfigurationBuilder cb, int index, AppConfig appConfig, StatisticsManager statsMgr) {
		this.cb = cb;
		this.threadName = THREAD_TYPE.NetworkFetcher.name() + index;
		this.appConfig = appConfig;
		this.statsMgr = statsMgr;
	}
	
	public void run() {
		Thread.currentThread().setName(threadName);
		logger.info("Starting network fetcher thread: " + threadName);
		
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
		DB 			 twitterDb 					= m.getDB(appConfig.getDBName());
		DBCollection usersColl 					= twitterDb.getCollection(TwitterCollections.users.name());
		DBCollection currentThreadsColl 		= twitterDb.getCollection(TwitterCollections.currentThreads.name());
		DBCollection usersGraphColl 			= twitterDb.getCollection(TwitterCollections.usersGraph.name());
		DBCollection usersGraphActionListColl 	= twitterDb.getCollection(TwitterCollections.usersGraphActionList.name());
//		DBCollection usersWaitingListColl 		= twitterDb.getCollection(TwitterCollections.usersWaitingList.name());
		Twitter 	 authenticatedTwitter		= new TwitterFactory(cb.build()).getInstance();
		
		// Register the thread in the currentThreads table
		DBObject threadObj = new BasicDBObject(currentThreads_SCHEMA.type.name(), THREAD_TYPE.NetworkFetcher.name())
			.append(currentThreads_SCHEMA.name.name(), threadName);
		currentThreadsColl.save(threadObj);
		
		// Get a queue of uids and process each member
		Queue<Long> uids = getNewList(usersColl, currentThreadsColl);
		while (true) {
			if(uids.size() == 0) {
				uids = getNewList(usersColl, currentThreadsColl);
				
				if(uids.size() == 0) {
					logger.error("Empty queue received!");
					threadObj.put(currentThreads_SCHEMA.status.name(), "closed");
					currentThreadsColl.save(threadObj);
					break;
				}
			}
			
			long uid = uids.poll();
			
			try {
				TimeUnit.SECONDS.sleep(3);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			// Check if the user has been covered already by another thread and should be covered in the next iteration
			DBObject usr = null;
			DBCursor usrs = usersColl.find(new BasicDBObject(users_SCHEMA.uid.name(), uid));
			if (usrs.count() == 0) {
				logger.error("User not found with uid: " + uid + ". This should not happen as the queue was populated from this collection only.");
				continue;
			} else if (usrs.count() == 1) {
				usr = usrs.next();
			} else {	// Duplicate users found
				logger.info("Duplicate entries found for uid: " + uid);
				int count = 0;
				List<DBObject> usrsToBeRemoved = new ArrayList<DBObject>();
				while (usrs.hasNext()) {
					DBObject usrC = usrs.next();
					if (count == 0)
						usr = usrC;
					else {
						// Store the duplicates to be removed later
						usrsToBeRemoved.add(usrC);
					}
					count++;
				}
				// Delete the duplicates
				for (DBObject usrD : usrsToBeRemoved)
					usersColl.remove(usrD);
			}
				
			if (usr == null) continue;
			int itr = Integer.parseInt(usr.get(users_SCHEMA.graphIterationCounter.name()).toString());
			
			int friendDepth = Integer.parseInt(usr.get(users_SCHEMA.friendDepth.name()).toString());
			int followerDepth = Integer.parseInt(usr.get(users_SCHEMA.followerDepth.name()).toString());
			// Mark it for the next iteration. Increment by more than 3 if it was 0. graphIterationCounter is initially:
			// 0 is for the seed/main users
			// 1 is for the mentioned users
			// 2 is if the profile changed for a user
			int nextItr = (itr == 0 || itr == 1 || itr == 2) ? itr + 3 : itr+1;
			
			if (friendDepth == 0 && followerDepth == 0)
				nextItr = 9999999;
			usr.put(users_SCHEMA.graphIterationCounter.name(), nextItr);
			usersColl.save(usr);
			
			// Check if it is being done by any other user currently
			if (currentThreadsColl.find(new BasicDBObject(currentThreads_SCHEMA.userId.name(), uid)).count() != 0) {
				// logger.debug("User's currently in use by another thread!" + uid);
				continue;
			}
				
			// Store the user as current user in the currentThreads table for this thread
			threadObj.put(currentThreads_SCHEMA.userId.name(), uid);
			currentThreadsColl.save(threadObj);
			
			// Process
			logger.info("Getting network for the user: " + uid);
			UserNetworkFetcher f = new UserNetworkFetcher(uid, 
					friendDepth, followerDepth);
			boolean success = f.fetchAndStoreInDB(usersGraphColl, usersGraphActionListColl, authenticatedTwitter, 
					currentThreadsColl, threadObj, usersColl);
			if (success) {
				usr.put("onceDone", true); // for debugging purposes
				usr.put(users_SCHEMA.followerCount.name(), f.getFollowerCount());
				
				// Add to the stats
				statsMgr.addGraphLiskUserTraversed(uid);
			} else {
				usr.put(users_SCHEMA.graphFetcherProblem.name(), true);
			}
			
			// Clear the current user from the currentThreads table
			threadObj.put(currentThreads_SCHEMA.userId.name(), "");
			currentThreadsColl.save(threadObj);
		}
		m.close();
	}

	private Queue<Long> getNewList(DBCollection usersListColl, DBCollection currentThreadsColl) {
		Queue<Long> ids = new LinkedList<Long>();
		
		DBCursor c = usersListColl.find()
						.sort(new BasicDBObject(users_SCHEMA.graphIterationCounter.name(), 1))
						.limit(USER_LIST_COUNT);
		
		while (c.hasNext()) {
			DBObject user = c.next();
        	if (!user.containsField(users_SCHEMA.uid.name()))
        		continue;
        	Double d = Double.parseDouble(user.get(users_SCHEMA.uid.name()).toString());
        	long uid = d.longValue();
        	ids.add(uid);
		}
		// Remove the ids currently being covered by other threads
		DBCursor cCurrThreads = currentThreadsColl.find(new BasicDBObject(currentThreads_SCHEMA.type.name(), THREAD_TYPE.NetworkFetcher.name()));
		while (cCurrThreads.hasNext()) {
			DBObject thread = cCurrThreads.next();
			try {
				Double d = Double.parseDouble(thread.get(currentThreads_SCHEMA.userId.name()).toString());
	        	long uid = d.longValue();
	        	ids.remove(new Long(uid));
			} catch (Exception e) {
				logger.debug("Valid user id not found!");
				continue;
			}
		}
		return ids;
	}
}
