package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

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
import edu.isi.db.TwitterMongoDBHandler;

public class UsersHistoricalTweetsFetcherThread implements Runnable {
	private int startIndex;
	private int endIndex;
	private Twitter authenticatedTwitter;
	
	private static int MAX_DAYS_TO_WAIT_FOR_NEXT_QUERY = 14;
	private static int TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS = 14;
	
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
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
		
		if(m == null) {
			System.out.println("Error getting connection to MongoDB! Cannot proceed with this thread.");
			return;
		}
		
		DB twitterDb = m.getDB(TwitterMongoDBHandler.DB_NAME);
		// DBCollection userColl = twitterDb.getCollection("users");
		DBCollection userColl = twitterDb.getCollection("SingleUserTest");
//		DBCollection tweetsColl = twitterDb.getCollection("tweets");
		DBCollection tweetsColl = twitterDb.getCollection("tweetstest");
		DBCollection usersFromTweetMentionsColl = twitterDb.getCollection("usersFromTweetMentions");
		
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
            	Double d = Double.parseDouble(user.get("uid").toString());
            	long uid = d.longValue();
            	
            	System.out.println("User: " + user.get("name"));
            	
            	/** Check if the user's timeline needs to be updated **/
            	boolean userUpdateRequired = isUserUpdateRequired(user); 
            	if(!userUpdateRequired)
            		continue;
            	
            	/** Fetch the user's timeline **/
            	UserTimelineFetcher f = new UserTimelineFetcher(uid, authenticatedTwitter);
            	boolean success = f.fetchAndStoreInDB(tweetsColl, userColl, usersFromTweetMentionsColl, false);
            	System.out.println("Success value " + success);
            	if (success) {
            		try {
            			user.put("lastUpdated", new Date());
            			/** Calculate the time after which it should be updated again **/
            			long nextUpdateAftervalue = getNextUpdateValueForTheUser(tweetsColl, uid);
            			user.put("nextUpdateAfter", nextUpdateAftervalue);
            			
            			/** Save the id of the user's latest tweet **/
            			BasicDBObject queryForLastTweet = new BasicDBObject();
            			queryForLastTweet.put("user.id", uid);
            			DBCursor cur = tweetsColl.find(queryForLastTweet).sort(new BasicDBObject("tweetCreatedAt", -1)).limit(1);
            			if(cur.hasNext()) {
            				Object latestTweetMaxId = cur.next().get("id");
            				if(latestTweetMaxId instanceof Long)
            					user.put("currentMaxId", latestTweetMaxId);
            			}
            	        
                		userColl.save(user);
            		} catch (MongoException me) {
            			System.out.println("Error saving user's last updated time stamp.");
            			System.err.println(me.getMessage());
            		}
            	}
            }
        } finally {
            cursor.close();
        }
        m.close();
	}

	private boolean isUserUpdateRequired(DBObject user) {
		if(user.containsField("lastUpdated") && user.containsField("nextUpdateAfter")) {
    		DateTime d = new DateTime(user.get("lastUpdated"));
    		long nextUpdatedDuration = Long.parseLong(user.get("nextUpdateAfter").toString());
    		DateTime dateToBeUpdated = new DateTime(d.getMillis() + nextUpdatedDuration);
    		
    		System.out.println("LastUpdatedDate" + d.toDate());
    		System.out.println("Date to be updated: " + dateToBeUpdated.toDate());
    		
    		if(dateToBeUpdated.isBefore(new DateTime().getMillis()))
    			return true;
    		else
    			return false;
    	} else
    		return true;
	}

	private long getNextUpdateValueForTheUser(DBCollection tweetsColl, long uid) {
		// Today's time
		Calendar date = Calendar.getInstance();
		System.out.println("Todays date: " + date.getTime());

		// 2 week's before time
		Calendar cldr = (Calendar) date.clone();
		cldr.add(Calendar.HOUR, -(24* TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS));
		System.out.println("Before date: " + cldr.getTime());
		
		// Count the number of tweets user had in last 2 week
		BasicDBObject query = new BasicDBObject();
        query.put("tweetCreatedAt", new BasicDBObject("$gte", cldr.getTime()).append("$lte", date.getTime()));
        query.put("user.id", uid);
		System.out.println("Number of tweets in 2 weeks: " + tweetsColl.find(query).count());
		int numberOfTweets = tweetsColl.find(query).count();
		
		// Calculate time required by the user to reach 200 tweets
		long t = 0L;
		if(numberOfTweets != 0)
			t = (TimeUnit.DAYS.toMillis(TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS)/(numberOfTweets)) * 200;
		
		if(t > TimeUnit.DAYS.toMillis(MAX_DAYS_TO_WAIT_FOR_NEXT_QUERY))
			return TimeUnit.DAYS.toMillis(MAX_DAYS_TO_WAIT_FOR_NEXT_QUERY);
		else
			return t;
	}

}