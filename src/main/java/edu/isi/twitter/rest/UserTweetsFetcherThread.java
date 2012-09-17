package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
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
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class UserTweetsFetcherThread implements Runnable {

	private int iteration;
	private ConfigurationBuilder cb;
	private String threadName;
	private Logger logger = LoggerFactory.getLogger(UserNetworkFetcherThread.class);
	private static int USER_LIST_COUNT = 50;
	
	private static int MAX_DAYS_TO_WAIT_FOR_NEXT_QUERY = 14;
	private static int TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS = 14;
	
	public UserTweetsFetcherThread(int iteration, ConfigurationBuilder cb, int index) {
		this.iteration = iteration;
		this.cb = cb;
		this.threadName = "TweetFetcher" + index;
	}
	
	public void run() {
		Thread.currentThread().setName(threadName);
		logger.info("Starting tweet fetcher thread: " + threadName);
		
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
		DBCollection usersColl = twitterDb.getCollection(TwitterCollections.users.name());
		DBCollection tweetsColl = twitterDb.getCollection(TwitterCollections.tweets.name());
		DBCollection usersFromTweetMentionsColl = twitterDb.getCollection(TwitterCollections.usersFromTweetMentions.name());
		DBCollection currentThreadsColl = twitterDb.getCollection(TwitterCollections.currentThreads.name());
		Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
		
		// Register the thread in the currentThreads table
		DBObject threadObj = new BasicDBObject("type", "TweetFetcher").append("name", threadName);
		currentThreadsColl.save(threadObj);
		
		// Get a queue of uids and process each member
		Queue<Long> uids = getNewList(usersColl, currentThreadsColl);
		while (true) {
			if(uids.size() == 0) {
				uids = getNewList(usersColl, currentThreadsColl);
				// Done with this iteration. Increment the iteration counter
				if(uids.size() == 0) {
					logger.info("Empty queue received. Incrementing the iteration number: " + (iteration+1));
					iteration++;
					uids = getNewList(usersColl, currentThreadsColl);
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
			DBCursor usrs = usersColl.find(new BasicDBObject("uid", uid));
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
			int itr = Integer.parseInt(usr.get("iteration").toString());
			if (itr > iteration) {
				logger.debug("User's itr greater than current iteration!");
				continue;
			}
				
			
			// Check if it is being done by any other user currently
			if (currentThreadsColl.find(new BasicDBObject("userId", uid)).count() != 0) {
				logger.debug("User's currently in use by another thread!");
				continue;
			}
				
			
			// Store the user as current user in the currentThreads table for this thread
			threadObj.put("userId", uid);
			currentThreadsColl.save(threadObj);
			
			// Process
			/** Check if the user's timeline needs to be updated **/
        	boolean userUpdateRequired = isUserUpdateRequired(usr);
        	if(!userUpdateRequired) {
        		logger.info("No timeline update currently required for " + usr.get("name"));
        		continue;
        	}
        		
        	
        	/** Fetch the user's timeline **/
        	logger.info("Fetching tweets for userid:" + uid);
        	UserTimelineFetcher f = new UserTimelineFetcher(uid, authenticatedTwitter);
        	boolean success = f.fetchAndStoreInDB(tweetsColl, usersColl, usersFromTweetMentionsColl, currentThreadsColl, threadObj, false);
        	if (success) {
        		try {
        			usr.put("lastUpdated", new Date());
        			/** Calculate the time after which it should be updated again **/
        			long nextUpdateAftervalue = getNextUpdateValueForTheUser(f.getNumberOfTweetsInLast2Weeks());
        			usr.put("nextUpdateAfter", nextUpdateAftervalue);
        	        usr.put("onceDone", true);
            		usersColl.save(usr);
        		} catch (MongoException me) {
        			logger.error("Error saving user's last updated time stamp.", me);
        		}
        	} else {
        		usr.put("problemOccured", true);
        		usersColl.save(usr);
        	}
			
			// Mark it for the next iteration
			usr.put("iteration", itr+1);
			usersColl.save(usr);
			
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
		DBCursor cCurrThreads = currentThreadsColl.find(new BasicDBObject("type", "TweetFetcher"));
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
		logger.info("Queue: " + ids);
		return ids;
	}
	
	private boolean isUserUpdateRequired(DBObject user) {
		if(user.containsField("lastUpdated") && user.containsField("nextUpdateAfter")) {
    		DateTime d = new DateTime(user.get("lastUpdated"));
    		long nextUpdatedDuration = Long.parseLong(user.get("nextUpdateAfter").toString());
    		DateTime dateToBeUpdated = new DateTime(d.getMillis() + nextUpdatedDuration);
    		
    		logger.debug("LastUpdatedDate" + d.toDate());
    		logger.debug("Date to be updated: " + dateToBeUpdated.toDate());
    		
    		if(dateToBeUpdated.isBefore(new DateTime().getMillis()))
    			return true;
    		else
    			return false;
    	} else
    		return true;
	}

	private long getNextUpdateValueForTheUser(int numberOfTweetsInLast2Weeks) {
		// Calculate time required by the user to reach 200 tweets
		long t = 0L;
		if(numberOfTweetsInLast2Weeks != 0)
			t = (TimeUnit.DAYS.toMillis(TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS)/(numberOfTweetsInLast2Weeks)) * 200;
		
		if(t > TimeUnit.DAYS.toMillis(MAX_DAYS_TO_WAIT_FOR_NEXT_QUERY))
			return TimeUnit.DAYS.toMillis(MAX_DAYS_TO_WAIT_FOR_NEXT_QUERY);
		else
			return t;
	}

}
