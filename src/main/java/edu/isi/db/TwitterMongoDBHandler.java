package edu.isi.db;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class TwitterMongoDBHandler {
	
	private static Logger logger = LoggerFactory.getLogger(TwitterMongoDBHandler.class);
	
	public enum TwitterCollections {
		users, tweets, usersWaitingList, usersGraph, usersGraphActionList, seedUsers, 
		currentThreads, applications, timezones, countryCodes, seedHashTags
	}
	
	public enum users_SCHEMA {
		// Related to user's information
		uid, name, location, timezone, passedGeospatialFilter, iteration, source,
		// Related to tweet fetcher threads
		lastUpdatedTweetFetcher, nextUpdateTweetFetcherDate, tweetFetcherProblem, tweetsPerDay, followMentions,
		// Related to user graph threads
		graphIterationCounter, friendDepth, followerDepth, graphFetcherProblem, followerCount
	}
	
	public enum usersGraph_SCHEMA {
		uid, link_user_id, link_type, first_seen, last_seen
	}
	
	public enum usersGraphActionList_SCHEMA {
		uid, link_user_id, link_type, date, action
	}
	
	public enum usersgraphlist_SCHEMA {
		uid, iteration, onceDone, name
	}
	
	public enum timezones_SCHEMA {
		name
	}
	
	public enum countryCodes_SCHEMA {
		name, countryCode
	}
	
	public enum seedHashTags_SCHEMA {
		value, iteration
	}
	
	public enum seedUsers_SCHEMA {
		uid, name
	}
	
	public enum currentThreads_SCHEMA {
		type, name, userId, status
	}
	
	public enum usersWaitingList_SCHEMA {
		uid, name, parsed, source, location, timezone, created, isGeoEnabled, friendDepth, followerDepth, followMentions, profileCompleteAndChecked, missingProfile
	}
	
	public enum THREAD_TYPE {
		NetworkFetcher, TweetFetcher
	}
	
	public enum USER_SOURCE {
		Graph, Mentions, Seed
	}
	
	public static String[] getSeedHashTagsList(String dbName) throws UnknownHostException, MongoException {
		Mongo m = MongoDBHandler.getNewMongoConnection();
		DB db = m.getDB(dbName);
		DBCollection coll = db.getCollection(TwitterCollections.seedHashTags.name());
		
		List<String> seedTags = new ArrayList<String>();
		DBCursor cursor = coll.find().snapshot();
		
        try {
            while(cursor.hasNext()) {
            	DBObject obj = cursor.next();
            	if(obj.containsField(seedHashTags_SCHEMA.value.name())) {
            		seedTags.add(obj.get(seedHashTags_SCHEMA.value.name()).toString());
            	}
            }
            logger.debug("List of hashtags: " + seedTags + ". \nSize: " + seedTags.size());
        } finally {
            cursor.close();
            m.close();
        }
        
        if(seedTags.size() == 0)
        	return new String[0];
        String[] arr = new String[seedTags.size()];
        int i = 0;
        for (String userId: seedTags) {
        	arr[i++] = userId;
        }
        return arr;
	}
	
	public static long[] getCurrentFollowUserIdList(String dbName, int maxNumberOfUsers) throws UnknownHostException, MongoException {
		Mongo m = MongoDBHandler.getNewMongoConnection();
		DB db = m.getDB(dbName);
		int userCounter = 0;
		DBCollection coll = db.getCollection(TwitterCollections.users.name());
		Set<Long> userIds = new HashSet<Long>();
		
		/** First add the seed users to the list **/
		DBCursor cursor = coll.find(new BasicDBObject(users_SCHEMA.source.name(), USER_SOURCE.Seed.name()));
	    while(cursor.hasNext()) {
        	DBObject obj = cursor.next();
        	if(obj.containsField(users_SCHEMA.uid.name())) {
        		userIds.add(Long.parseLong(obj.get(users_SCHEMA.uid.name()).toString()));
        		userCounter++;
        		if (userCounter >= maxNumberOfUsers) {
        			cursor.close();
        			m.close();
        			return convertListToArray(userIds);
        		}
        	}
        }
        cursor.close();
        
        /** Then add the users that have the passedGeospatialFilter flag set to true **/
		cursor = coll.find(new BasicDBObject(users_SCHEMA.passedGeospatialFilter.name(), true)).snapshot();
		while(cursor.hasNext()) {
        	DBObject obj = cursor.next();
        	if(obj.containsField(users_SCHEMA.uid.name())) {
        		userIds.add(Long.parseLong(obj.get(users_SCHEMA.uid.name()).toString()));
        		userCounter++;
        		if (userCounter >= maxNumberOfUsers) {
        			cursor.close();
        			m.close();
        			return convertListToArray(userIds);
        		}
        	}
        }
        logger.debug("List of users to be followed by the stream: " + userIds + ". \nSize: " + userIds.size());
        
        /** Convert the list to array and return the array **/
        cursor.close();
		m.close();
        return convertListToArray(userIds);
	}

	private static long[] convertListToArray(Set<Long> userIds) {
		long[] arr = new long[userIds.size()];
        int i = 0;
        for (Long userId: userIds) {
        	arr[i++] = userId;
        }
        return arr;
	}
	
	/*
	public static void addToUsersCollection(UserMentionEntity[] mentionEntities, DBCollection userColl, DBCollection usersFromTweetMentionsColl) {
		for (UserMentionEntity userMention : mentionEntities) {
			try {
				long uid = userMention.getId();
				BasicDBObject userObj = new BasicDBObject();
				userObj.put("uid", new Long(uid).doubleValue());
				userObj.put("name", userMention.getScreenName());
				userObj.put("addedFromTweetMentions", true);
				userObj.put("incomplete", 1);
				usersFromTweetMentionsColl.save(userObj);
			} catch (Exception e) {
				logger.debug("Error occured while adding tweet mention user: " + userMention.getName() + ". Error: " + e.getMessage());
				continue;
			}
		}
	}
	*/
}
