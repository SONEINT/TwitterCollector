package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
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
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;

public class UsersHistoricalTweetsFetcherThread implements Runnable {
	private int startIndex;
	private int endIndex;
	private Twitter authenticatedTwitter;
	
	private static int MAX_DAYS_TO_WAIT_FOR_NEXT_QUERY = 14;
	private static int TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS = 14;
	
	private Logger logger = LoggerFactory.getLogger(UsersHistoricalTweetsFetcherThread.class);
	
	public UsersHistoricalTweetsFetcherThread(int startIndex, int endIndex, ConfigurationBuilder cb) {
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
	}

	public void run() {
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
		DBCollection userColl = twitterDb.getCollection(TwitterCollections.users.name());
		DBCollection tweetsColl = twitterDb.getCollection(TwitterCollections.tweets.name());
		DBCollection usersFromTweetMentionsColl = twitterDb.getCollection(TwitterCollections.usersFromTweetMentions.name());
		
        //query.put("uid", new BasicDBObject("$gte", startIndex).append("$lte", endIndex)); // Useless code
		
        DBCursor cursor = userColl.find();
        cursor.skip(startIndex);
        int userCounter = startIndex;
        try {
            while(cursor.hasNext()) {
            	if(userCounter++>endIndex) {
            		break;
            	}
            	
            	DBObject user = cursor.next();
            	if (!user.containsField("uid"))
            		continue;
            	Double d = Double.parseDouble(user.get("uid").toString());
            	long uid = d.longValue();
            	
            	logger.info("Current user for fetching timeline: " + user.get("name"));
            	
            	/** Check if the user's timeline needs to be updated **/
            	boolean userUpdateRequired = isUserUpdateRequired(user);
            	if(!userUpdateRequired) {
            		logger.info("No timeline update currently required for " + user.get("name"));
            		continue;
            	}
            		
            	
            	/** Fetch the user's timeline **/
            	UserTimelineFetcher f = new UserTimelineFetcher(uid, authenticatedTwitter);
            	boolean success = f.fetchAndStoreInDB(tweetsColl, userColl, usersFromTweetMentionsColl, false);
            	if (success) {
            		try {
            			user.put("lastUpdated", new Date());
            			/** Calculate the time after which it should be updated again **/
            			long nextUpdateAftervalue = getNextUpdateValueForTheUser(f.getNumberOfTweetsInLast2Weeks());
            			user.put("nextUpdateAfter", nextUpdateAftervalue);
            			
//            			/** Save the id of the user's latest tweet **/
//            			BasicDBObject queryForLastTweet = new BasicDBObject();
//            			queryForLastTweet.put("user.id", uid);
//            			DBCursor cur = tweetsColl.find(queryForLastTweet).sort(new BasicDBObject("tweetCreatedAt", -1)).limit(1);
//            			if(cur.hasNext()) {
//            				Object latestTweetMaxId = cur.next().get("id");
//            				if(latestTweetMaxId instanceof Long)
//            					user.put("currentMaxId", latestTweetMaxId);
//            			}
            	        user.put("onceDone", true);
                		userColl.save(user);
            		} catch (MongoException me) {
            			logger.error("Error saving user's last updated time stamp.", me);
            		}
            	}
            }
        } finally {
            cursor.close();
        }
        m.close();
        logger.info("Finished getting tweets for the application responsible for start index: " + startIndex + " and the end index: " + endIndex);
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
