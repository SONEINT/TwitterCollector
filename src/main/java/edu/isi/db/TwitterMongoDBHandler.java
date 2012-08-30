package edu.isi.db;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.UserMentionEntity;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class TwitterMongoDBHandler {
	
	private static Logger logger = LoggerFactory.getLogger(TwitterMongoDBHandler.class);
	
	public enum TwitterApplication {
		twitter
	}
	
	public enum TwitterCollections {
		users, tweets, usersFromTweetMentions, usersGraph, usersGraphActionList, usersgraphlist, tweetsFromStream
	}
	
	public static long[] getCurrentFollowUserIdList() throws UnknownHostException, MongoException {
		Mongo m = MongoDBHandler.getNewMongoConnection();
		DB db = m.getDB(TwitterApplication.twitter.name());
		
		BasicDBObject query = new BasicDBObject();
		query.put("follow", 1);
		DBCollection coll = db.getCollection(TwitterCollections.users.name());
		
		List<Long> userIds = new ArrayList<Long>();
		DBCursor cursor = coll.find(query);
		
        try {
            while(cursor.hasNext()) {
            	DBObject obj = cursor.next();
            	if(obj.containsField("uid")) {
            		userIds.add(Long.parseLong(obj.get("uid").toString()));
            	}
            }
            logger.info("List of users to be followed by the stream: " + userIds + ". \nSize: " + userIds.size());
        } finally {
            cursor.close();
        }
        
        long[] arr = new long[userIds.size()];
        int i = 0;
        for (Long userId: userIds) {
        	arr[i++] = userId;
        }
        m.close();
        return arr;
	}
	
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
				logger.info("Error occured while adding tweet mention user: " + userMention.getName() + ". Error: " + e.getMessage());
				continue;
			}
		}
	}
}
