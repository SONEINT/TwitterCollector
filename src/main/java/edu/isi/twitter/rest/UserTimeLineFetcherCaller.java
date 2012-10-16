package edu.isi.twitter.rest;

import java.net.UnknownHostException;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;

public class UserTimeLineFetcherCaller {
	
	public static String ACCESS_TOKEN = "750028153-PWyGgHuXrqvKl7eOc0UkKd6aeYDvKMmgqm6g2J2s";
	public static String ACCESS_TOKEN_SECRET = "uEjZ8MS4jju35hdMiR5125rYrGcwMFrbwLtP3j8b7Bg";
	public static String CONSUMER_KEY = "DpSC9yFJUl9EQTBexmCxiw";
	public static String CONSUMER_SECRET = "RgVnqf9tb6PlIYLoswgeuOq695bMZGsgmDxeFndcTA";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		ConfigurationBuilder cb = new ConfigurationBuilder()
			.setDebugEnabled(true)
			.setOAuthConsumerKey(CONSUMER_KEY)
	    	.setOAuthConsumerSecret(CONSUMER_SECRET)
	    	.setOAuthAccessToken(ACCESS_TOKEN)
	    	.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
			.setJSONStoreEnabled(true);
	
		Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
		
		Mongo m;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
			DB twitterDb = m.getDB("twitter");
			DBCollection usersColl = twitterDb.getCollection(TwitterCollections.users.name());
			DBCollection tweetsColl = twitterDb.getCollection("tweetsTest");
			DBCollection usersWaitingListColl = twitterDb.getCollection(TwitterCollections.usersWaitingList.name());
			DBCollection currentThreadsColl = twitterDb.getCollection(TwitterCollections.currentThreads.name());
			
			DBObject threadObj = new BasicDBObject("type", "TweetFetcher").append("name", "testThread");
			currentThreadsColl.save(threadObj);
			
			System.out.println("Starting now...");
			UserTimelineFetcher f = new UserTimelineFetcher(40885516, authenticatedTwitter, false);
        	f.fetchAndStoreInDB(tweetsColl, usersColl, usersWaitingListColl, currentThreadsColl, threadObj, false);
			
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
