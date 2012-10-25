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
		// Seed collections
		seedUsers, seedHashTags,
		
		// Data storing collections
		users, tweets, usersGraph, usersGraphActionList,  
		
		// Application management collections
		usersWaitingList, currentThreads, applications, timezones, countryCodes,
		
		// Statistics collections
		tweetsStats, linksStats,
		
		// Store relationships
		tweetsLog, replyToTable, hashTagTweetsTable, mentionsTable 
	}
	
	public enum users_SCHEMA {
		// Related to user's information
		uid, name, location, timezone, passedGeospatialFilter, iteration, source,
		// Related to tweet fetcher threads
		lastUpdatedTweetFetcher, nextUpdateTweetFetcherDate, tweetFetcherProblem, tweetsPerDay, followMentions, timelineTweetsMaxId,
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
		uid, name, parsed, source, location, timezone, created, isGeoEnabled, friendDepth, 
		followerDepth, followMentions, profileCompleteAndChecked, missingProfile, linkType
	}
	
	public enum tweets_SCHEMA {
		APISource, tweetCreatedAt, id
	}
	
	public enum applications_SCHEMA {
		tag, user_id, access_token, access_token_secret, consumer_key, consumer_key_secret
	}
	
	public enum tweetsStats_SCHEMA {
		time, timeReadable, count
	}
	
	public enum linksStats_SCHEMA {
		time, timeReadable, count
	}
	
	public enum tweetsLog_SCHEMA {
		tweetId, source, insertionDate, tweetCreationDate
	}
	
	public enum replyToTable_SCHEMA {
		tweetID, sourceTweetID, uid, parentUid, insertionDate, tweetCreationDate
	}
	
	public enum hashTagTweetTable_SCHEMA {
		tweetID, tag, uid, insertionDate, tweetCreationDate
	}
	
	public enum mentionsTable_SCHEMA {
		tweetID, uid, mentionUid, insertionDate, tweetCreationDate
	}
	
	public enum THREAD_TYPE {
		NetworkFetcher, TweetFetcher
	}
	
	public enum USER_SOURCE {
		Graph, Mentions, Seed
	}
	
	public enum TWEET_SOURCE {
		Timeline, Streaming, Search
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
	
	public static void createCollectionsAndIndexes(String dbName) throws UnknownHostException, MongoException {
		Mongo m = MongoDBHandler.getNewMongoConnection();
		DB db = m.getDB(dbName);
		
		/** Create all the required collections **/
		DBCollection usersColl 					= db.getCollection(TwitterCollections.users.name());
		DBCollection tweetsColl 				= db.getCollection(TwitterCollections.tweets.name());
		DBCollection usersWaitingListColl 		= db.getCollection(TwitterCollections.usersWaitingList.name());
		DBCollection usersGraphColl 			= db.getCollection(TwitterCollections.usersGraph.name());
		DBCollection usersGraphActionListColl 	= db.getCollection(TwitterCollections.usersGraphActionList.name());
		DBCollection applicationsColl 			= db.getCollection(TwitterCollections.applications.name());
		DBCollection tweetsLogColl 				= db.getCollection(TwitterCollections.tweetsLog.name());
		DBCollection replyToColl 				= db.getCollection(TwitterCollections.replyToTable.name());
		DBCollection hashtagsTweetColl 			= db.getCollection(TwitterCollections.hashTagTweetsTable.name());
		DBCollection mentionsColl 				= db.getCollection(TwitterCollections.mentionsTable.name());
		
		db.getCollection(TwitterCollections.currentThreads.name());

		/** Create indexes */
		DBObject uniqueIndexOption = new BasicDBObject("unique", true).append("dropDups", true);
		usersColl.ensureIndex(new BasicDBObject(users_SCHEMA.uid.name(), 1), uniqueIndexOption);
		usersWaitingListColl.ensureIndex(new BasicDBObject(usersWaitingList_SCHEMA.uid.name(), 1), uniqueIndexOption);
		tweetsColl.ensureIndex(new BasicDBObject(tweets_SCHEMA.id.name(), 1), uniqueIndexOption);
		tweetsColl.ensureIndex(new BasicDBObject("user.id", 1), uniqueIndexOption);
		usersGraphColl.ensureIndex(new BasicDBObject(usersGraph_SCHEMA.uid.name(), 1));
		usersGraphActionListColl.ensureIndex(new BasicDBObject(usersGraphActionList_SCHEMA.uid.name(), 1));
		applicationsColl.ensureIndex(new BasicDBObject(applications_SCHEMA.user_id.name(), 1), uniqueIndexOption);
		
		
		tweetsLogColl.ensureIndex(new BasicDBObject(tweetsLog_SCHEMA.tweetId.name(), 1), uniqueIndexOption);
		tweetsLogColl.ensureIndex(new BasicDBObject(tweetsLog_SCHEMA.source.name(), 1));
		tweetsLogColl.ensureIndex(new BasicDBObject(tweetsLog_SCHEMA.insertionDate.name(), 1));
		
		replyToColl.ensureIndex(new BasicDBObject(replyToTable_SCHEMA.uid.name(), 1));
		replyToColl.ensureIndex(new BasicDBObject(replyToTable_SCHEMA.parentUid.name(), 1));
		replyToColl.ensureIndex(new BasicDBObject(replyToTable_SCHEMA.tweetCreationDate.name(), 1));
		
		hashtagsTweetColl.ensureIndex(new BasicDBObject(hashTagTweetTable_SCHEMA.tag.name(), 1));
		hashtagsTweetColl.ensureIndex(new BasicDBObject(hashTagTweetTable_SCHEMA.uid.name(), 1));
		hashtagsTweetColl.ensureIndex(new BasicDBObject(hashTagTweetTable_SCHEMA.tweetCreationDate.name(), 1));
		
		mentionsColl.ensureIndex(new BasicDBObject(mentionsTable_SCHEMA.uid.name(), 1));
		mentionsColl.ensureIndex(new BasicDBObject(mentionsTable_SCHEMA.mentionUid.name(), 1));
		mentionsColl.ensureIndex(new BasicDBObject(mentionsTable_SCHEMA.tweetCreationDate.name(), 1));
		
		m.close();
	}
	
	public static void addToTweetLogTable (DBCollection tweetLogColl, String source, long tweetId, 
			 long tweetInsertionDate, long tweetCreationDate) throws MongoException {
		tweetLogColl.insert(
				new BasicDBObject(tweetsLog_SCHEMA.tweetId.name(), tweetId)
				.append(tweetsLog_SCHEMA.source.name(), source)
				.append(tweetsLog_SCHEMA.insertionDate.name(), tweetInsertionDate)
				.append(tweetsLog_SCHEMA.tweetCreationDate.name(), tweetCreationDate));
	}
	
	public static void addToReplyToTable (DBCollection replyToColl, long tweetId, long sourceTweetId, long uid
			, long parentUid, long insertionDate, long tweetCreationDate) {
		replyToColl.insert(
				new BasicDBObject(replyToTable_SCHEMA.tweetID.name(), tweetId)
				.append(replyToTable_SCHEMA.sourceTweetID.name(), sourceTweetId)
				.append(replyToTable_SCHEMA.uid.name(), uid)
				.append(replyToTable_SCHEMA.parentUid.name(), parentUid)
				.append(replyToTable_SCHEMA.insertionDate.name(), insertionDate)
				.append(replyToTable_SCHEMA.tweetCreationDate.name(), tweetCreationDate)
				);
	}
	
	
	public static void addToMentionsTable (DBCollection mentionsColl, long tweetId, long uid, 
			long mentionUid, long insertionDate, long tweetCreationDate) {
		mentionsColl.insert(
				new BasicDBObject(mentionsTable_SCHEMA.tweetID.name(), tweetId)
				.append(mentionsTable_SCHEMA.uid.name(), uid)
				.append(mentionsTable_SCHEMA.mentionUid.name(), mentionUid)
				.append(mentionsTable_SCHEMA.insertionDate.name(), insertionDate)
				.append(mentionsTable_SCHEMA.tweetCreationDate.name(), tweetCreationDate));
	}
	
	public static void addTohashTagTweetsTable(DBCollection hashTagTweetsTable, String hashTag, long tweetId, long uid, long insertionDate, long tweetCreationDate) {
		hashTagTweetsTable.insert(
				new BasicDBObject(hashTagTweetTable_SCHEMA.tweetID.name(), tweetId)
				.append(hashTagTweetTable_SCHEMA.uid.name(), uid)
				.append(hashTagTweetTable_SCHEMA.tag.name(), hashTag)
				.append(hashTagTweetTable_SCHEMA.insertionDate.name(), insertionDate)
				.append(hashTagTweetTable_SCHEMA.tweetCreationDate.name(), tweetCreationDate));
		
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
