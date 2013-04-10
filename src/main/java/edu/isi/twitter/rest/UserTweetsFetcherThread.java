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

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
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

public class UserTweetsFetcherThread implements Runnable {

	private ConfigurationBuilder cb;
	private String threadName;
	private Logger logger = LoggerFactory.getLogger(UserNetworkFetcherThread.class);
	private static int USER_LIST_COUNT = 50;
	private AppConfig appConfig;
	private StatisticsManager statsMgr;
	
	private static int MAX_DAYS_TO_WAIT_FOR_NEXT_QUERY = 14;
	private static int TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS = 14;
	private static long CURRENT_USER_TIME_DELTA = 8640000000l; // 100 days
	
	
	public UserTweetsFetcherThread(ConfigurationBuilder cb, int index, AppConfig appConfig, StatisticsManager statsMgr) {
		this.cb = cb;
		this.threadName = THREAD_TYPE.TweetFetcher.name() + index;
		this.appConfig = appConfig;
		this.statsMgr = statsMgr;
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
		
		DB twitterDb = m.getDB(appConfig.getDBName());
		DBCollection usersColl = twitterDb.getCollection(TwitterCollections.users.name());
		DBCollection tweetsColl = twitterDb.getCollection(TwitterCollections.tweets.name());
		DBCollection usersWaitingListColl = twitterDb.getCollection(TwitterCollections.usersWaitingList.name());
		DBCollection currentThreadsColl = twitterDb.getCollection(TwitterCollections.currentThreads.name());
		DBCollection tweetsLogColl = twitterDb.getCollection(TwitterCollections.tweetsLog.name());
		DBCollection replyToColl = twitterDb.getCollection(TwitterCollections.replyToTable.name());
		DBCollection mentionsColl = twitterDb.getCollection(TwitterCollections.mentionsTable.name());
		Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
		
		// Register the thread in the currentThreads table
		DBObject threadObj = new BasicDBObject(currentThreads_SCHEMA.type.name(), THREAD_TYPE.TweetFetcher.name())
			.append(currentThreads_SCHEMA.name.name(), threadName);
		currentThreadsColl.save(threadObj);
		
		// Get a queue of uids and process each member
		Queue<Long> uids = getNewList(usersColl, currentThreadsColl);
		while (true) {
			if(uids.size() == 0) {
				uids = getNewList(usersColl, currentThreadsColl);
				if(uids.size() == 0) {
					logger.info("Empty queue received.");
					threadObj.put("status", "closed");
					currentThreadsColl.save(threadObj);
					break;
				}
			}
			
			long uid = uids.poll();
			
			try {
				TimeUnit.SECONDS.sleep(2);
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
			
			long nextUpdateAfterOldVal = Long.parseLong(usr.get(users_SCHEMA.nextUpdateTweetFetcherDate.name()).toString());
			// Increase its nextUpdateTweetFetcherDate value to so that it is not picked up by any other thread at the same time
			usr.put(users_SCHEMA.nextUpdateTweetFetcherDate.name(), nextUpdateAfterOldVal + CURRENT_USER_TIME_DELTA);
			usersColl.save(usr);
			
			// Check if it is being done by any other thread currently
			if (currentThreadsColl.find(new BasicDBObject(currentThreads_SCHEMA.userId.name(), uid)).count() != 0) {
//				logger.debug("User's currently in use by another thread!");
				continue;
			}
			
			// Store the user as current user in the currentThreads table for this thread
			threadObj.put(currentThreads_SCHEMA.userId.name(), uid);
			currentThreadsColl.save(threadObj);
			
			// Process
//			/** Check if the user's timeline needs to be updated **/
//        	boolean userUpdateRequired = isUserUpdateRequired(usr);
//        	if(!userUpdateRequired) {
//        		logger.info("No timeline update currently required for " + usr.get("name"));
//        		continue;
//        	}
        		
        	
        	/** Fetch the user's timeline **/
        	logger.info("Fetching tweets for userid:" + uid);
        	long timelineTweetMaxId = 0l;
        	if (usr.containsField(users_SCHEMA.timelineTweetsMaxId.name())) {
        		try {
        			timelineTweetMaxId = Long.parseLong(usr.get(users_SCHEMA.timelineTweetsMaxId.name()).toString());
        		} catch (Exception t) {
        			// Do nothing if there is an exception while parsing the field's data
        		}
        	}
        	
        	UserTimelineFetcher f = new UserTimelineFetcher(uid, authenticatedTwitter, appConfig.isFollowMentions(), timelineTweetMaxId);
        	boolean success = f.fetchAndStoreInDB(tweetsColl, usersWaitingListColl, currentThreadsColl, 
        			threadObj, tweetsLogColl, replyToColl, mentionsColl);
        	if (success) {
        		try {
        			usr.put(users_SCHEMA.lastUpdatedTweetFetcher.name(), new Date());
        			/** Calculate the date (in milliseconds) at which it should be updated again **/
        			long nextUpdateDuration = getNextUpdateValueForTheUser(f.getNumberOfTweetsInLast2Weeks());
        			DateTime dateToBeUpdated = new DateTime(new DateTime().getMillis() + nextUpdateDuration);
        			usr.put(users_SCHEMA.nextUpdateTweetFetcherDate.name(), dateToBeUpdated.getMillis());
        			usr.put(users_SCHEMA.tweetsPerDay.name(), getUsersActivityRate(f.getNumberOfTweetsInLast2Weeks()));
        			usr.put(users_SCHEMA.timelineTweetsMaxId.name(), f.getTimelineTweetMaxId());
        			
        	        usr.put("onceDone", true); // for debugging purposes
            		usersColl.save(usr);
            		
            		// Add to the stats
            		statsMgr.addTimelineUserTraversed(uid);
        		} catch (MongoException me) {
        			logger.error("Error saving user's last updated time stamp.", me);
        		}
        	} else {
        		usr.put(users_SCHEMA.tweetFetcherProblem.name(), true);
        		usersColl.save(usr);
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
						.sort(new BasicDBObject(users_SCHEMA.nextUpdateTweetFetcherDate.name(), 1))
						.limit(USER_LIST_COUNT).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		
		while (c.hasNext()) {
			DBObject user = c.next();
        	if (!user.containsField(users_SCHEMA.uid.name()))
        		continue;
        	Double d = Double.parseDouble(user.get(users_SCHEMA.uid.name()).toString());
        	long uid = d.longValue();
        	ids.add(uid);
		}
		// Remove the ids currently being covered by other threads
		DBCursor cCurrThreads = currentThreadsColl.find(new BasicDBObject(currentThreads_SCHEMA.type.name(), 
				THREAD_TYPE.TweetFetcher.name())).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
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
		// logger.info("Queue: " + ids);
		return ids;
	}
	
	/*
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
	*/

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
	
	private float getUsersActivityRate (int numberOfTweetsInLast2weeks) {
		return numberOfTweetsInLast2weeks/(float)TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS;
	}
}
