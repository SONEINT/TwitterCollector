package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

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
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.users_SCHEMA;
import edu.isi.twitter.TwitterApplicationManager;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;

public class Caller_UserTimeLineFetcher {
	
	public static String ACCESS_TOKEN = "750028153-PWyGgHuXrqvKl7eOc0UkKd6aeYDvKMmgqm6g2J2s";
	public static String ACCESS_TOKEN_SECRET = "uEjZ8MS4jju35hdMiR5125rYrGcwMFrbwLtP3j8b7Bg";
	public static String CONSUMER_KEY = "DpSC9yFJUl9EQTBexmCxiw";
	public static String CONSUMER_SECRET = "RgVnqf9tb6PlIYLoswgeuOq695bMZGsgmDxeFndcTA";

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		
//		ConfigurationBuilder cb = new ConfigurationBuilder()
//			.setDebugEnabled(true)
//			.setOAuthConsumerKey(CONSUMER_KEY)
//	    	.setOAuthConsumerSecret(CONSUMER_SECRET)
//	    	.setOAuthAccessToken(ACCESS_TOKEN)
//	    	.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
//			.setJSONStoreEnabled(true);
		ConfigurationBuilder cfg = TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.Search, "twitter");
		Twitter twitter = new TwitterFactory(cfg.build()).getInstance();
		
		Mongo m;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
			DB twitterDb = m.getDB("twitter");
			
			DBCollection usersColl = twitterDb.getCollection("usersSmallList");
			usersColl.insert(new BasicDBObject("uid", 37032481l));
			usersColl.insert(new BasicDBObject("uid", 811377l));
			
			DBCollection tweetsColl = twitterDb.getCollection("tweets");
			DBCollection usersWaitingListColl = twitterDb.getCollection(TwitterCollections.usersWaitingList.name());
			DBCollection currentThreadsColl = twitterDb.getCollection(TwitterCollections.currentThreads.name());
			DBCollection tweetsLogColl = twitterDb.getCollection(TwitterCollections.tweetsLog.name());
			DBCollection replyToColl = twitterDb.getCollection(TwitterCollections.replyToTable.name());
			DBCollection mentionsColl = twitterDb.getCollection(TwitterCollections.mentionsTable.name());
			DBObject threadObj = new BasicDBObject("type", "TweetFetcher").append("name", "testThread");
			currentThreadsColl.save(threadObj);
			
			System.out.println("Starting now...");
			
			int counter = 0;
			
			while (true) {
				System.out.println("starting agin ...");
				DBCursor c = usersColl.find().addOption(Bytes.QUERYOPTION_NOTIMEOUT);
				while (c.hasNext()) {
					DBObject usr = c.next();
					long uid = Long.parseLong(usr.get("uid").toString());
					System.out.println("UID: " + uid);
					long timelineTweetMaxId = 0l;
					
		        	if (usr.containsField(users_SCHEMA.timelineTweetsMaxId.name())) {
		        		try {
		        			timelineTweetMaxId = Long.parseLong(usr.get(users_SCHEMA.timelineTweetsMaxId.name()).toString());
		        			System.out.println("Parsed tweet max id from Mongo: " + timelineTweetMaxId);
		        		} catch (Exception t) {
		        			System.out.println("########################");
		        			// Do nothing if there is an exception while parsing the field's data
		        		}
		        	} else {
		        		System.out.println("No max tweet id found!");
		        	}
		        	
		        	UserTimelineFetcher f = new UserTimelineFetcher(uid, twitter, false, timelineTweetMaxId);
		        	boolean success = f.fetchAndStoreInDB(tweetsColl, usersWaitingListColl, currentThreadsColl, 
		        			threadObj, tweetsLogColl, replyToColl, mentionsColl);
		        	
		        	if (success) {
		        		try {
		        			usr.put(users_SCHEMA.timelineTweetsMaxId.name(), f.getTimelineTweetMaxId());
		        			usersColl.save(usr);
		        		} catch (MongoException me) {
		        			me.printStackTrace();
		        		}
		        	}
				}
				System.out.println("Sleeping ...");
				
				TimeUnit.SECONDS.sleep(5l);
				if (counter++ > 5)
					break;
			}
			
			
			// Testing updating of fields
//			DBCollection apps = db.getCollection("applications");
//			BasicDBObject query = new BasicDBObject();
//	        query.put("user_id", "deleteme");
//			DBObject obj = apps.findOne(query);
			
//			long l = Date.parse(obj.get("lastUpdated").toString());
//			System.out.println(l);
//			Date d = new Date(l);
//			System.out.println(d.toGMTString());
//			
//			System.out.println(TimeUnit.SECONDS.toMillis(3L));
//			Date now = new Date();
//			obj.put("lastUpdated", now);
//			apps.save(obj);
			// System.out.println(obj);
			
			m.close();
			System.out.println("Done");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
