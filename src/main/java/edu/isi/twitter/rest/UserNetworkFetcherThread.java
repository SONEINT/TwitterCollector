package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;

public class UserNetworkFetcherThread implements Runnable {
	
	private int iteration;
	private ConfigurationBuilder cb;
	private String threadName;
	private Logger logger = LoggerFactory.getLogger(UserNetworkFetcherThread.class);
	private static int USER_LIST_COUNT = 50;
	
	public UserNetworkFetcherThread(int iteration, ConfigurationBuilder cb, int index) {
		this.iteration = iteration;
		this.cb = cb;
		this.threadName = "NetworkFetcher" + index;
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
		DB twitterDb = m.getDB(TwitterApplication.twitter.name());
		DBCollection usersGraphListColl = twitterDb.getCollection(TwitterCollections.usersgraphlist.name());
		DBCollection currentThreadsColl = twitterDb.getCollection(TwitterCollections.currentThreads.name());
		DBCollection usersGraphColl = twitterDb.getCollection(TwitterCollections.usersGraph.name());
		DBCollection usersGraphActionListColl = twitterDb.getCollection(TwitterCollections.usersGraphActionList.name());
		Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
		
		// Register the thread in the currentThreads table
		DBObject threadObj = new BasicDBObject("type", "NetworkFetcher").append("name", threadName);
		currentThreadsColl.save(threadObj);
		
		// Get a queue of uids and process each member
		Queue<Long> uids = getNewList(usersGraphListColl, currentThreadsColl);
		while (true) {
			if(uids.size() == 0) {
				uids = getNewList(usersGraphListColl, currentThreadsColl);
				// Done with this iteration. Increment the iteration counter
				if(uids.size() == 0) {
					logger.info("Empty queue received. Incrementing the iteration number: " + (iteration+1));
					iteration++;
					uids = getNewList(usersGraphListColl, currentThreadsColl);
					if (uids.size() == 0) {
						logger.error("Something badly wrong! No uids retrieved for the iteration: " + iteration);
						threadObj.put("status", "closed");
						currentThreadsColl.save(threadObj);
						break;
					}
				}
			}
			
			long uid = uids.poll();
			
			// Check if the user has been covered already by another thread and should be covered in the next iteration
			DBObject usr = null;
			DBCursor usrs = usersGraphListColl.find(new BasicDBObject("uid", uid));
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
					usersGraphListColl.remove(usrD);
			}
				
			if (usr == null) continue;
			int itr = Integer.parseInt(usr.get("iteration").toString());
			if (itr > iteration) {
				logger.debug("User's itr greater than current iteration!" + uid);
				continue;
			}
				
			
			// Check if it is being done by any other user currently
			if (currentThreadsColl.find(new BasicDBObject("userId", uid)).count() != 0) {
				logger.debug("User's currently in use by another thread!" + uid);
				continue;
			}
				
			
			// Store the user as current user in the currentThreads table for this thread
			threadObj.put("userId", uid);
			currentThreadsColl.save(threadObj);
			
			// Process
			logger.info("Getting network for the user: " + uid);
			UserNetworkFetcher f = new UserNetworkFetcher(uid);
			boolean success = f.fetchAndStoreInDB(usersGraphColl, usersGraphActionListColl, authenticatedTwitter, currentThreadsColl, threadObj);
			if (success) {
				usr.put("onceDone", true);
			} else {
				usr.put("problemOccured", true);
			}
			
			// Mark it for the next iteration
			usr.put("iteration", itr+1);
			usersGraphListColl.save(usr);
			
			// Clear the current user from the currentThreads table
			threadObj.put("userId", "");
			currentThreadsColl.save(threadObj);
		}
		m.close();
	}

	private Queue<Long> getNewList(DBCollection usersListColl, DBCollection currentThreadsColl) {
		Queue<Long> ids = new LinkedList<Long>();
		
		DBCursor c = usersListColl.find(new BasicDBObject("iteration", new BasicDBObject("$lte", iteration)))
						.sort(new BasicDBObject("iteration", 1))
						.limit(USER_LIST_COUNT);
		
		while (c.hasNext()) {
			DBObject user = c.next();
        	if (!user.containsField("uid"))
        		continue;
        	Double d = Double.parseDouble(user.get("uid").toString());
        	long uid = d.longValue();
        	ids.add(uid);
		}
		// Remove the ids currently being covered by other threads
		DBCursor cCurrThreads = currentThreadsColl.find(new BasicDBObject("type", "NetworkFetcher"));
		while (cCurrThreads.hasNext()) {
			DBObject thread = cCurrThreads.next();
			try {
				Double d = Double.parseDouble(thread.get("userId").toString());
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
