package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

public class UserTimeLineFetcherCaller {
	
	public static String ACCESS_TOKEN = "750028153-PWyGgHuXrqvKl7eOc0UkKd6aeYDvKMmgqm6g2J2s";
	public static String ACCESS_TOKEN_SECRET = "uEjZ8MS4jju35hdMiR5125rYrGcwMFrbwLtP3j8b7Bg";
	public static String CONSUMER_KEY = "DpSC9yFJUl9EQTBexmCxiw";
	public static String CONSUMER_SECRET = "RgVnqf9tb6PlIYLoswgeuOq695bMZGsgmDxeFndcTA";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
//		ConfigurationBuilder cb = new ConfigurationBuilder()
//			.setDebugEnabled(true)
//			.setOAuthConsumerKey(CONSUMER_KEY)
//	    	.setOAuthConsumerSecret(CONSUMER_SECRET)
//	    	.setOAuthAccessToken(ACCESS_TOKEN)
//	    	.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
//			.setJSONStoreEnabled(true);
//	
//		Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
		
		Mongo m;
		try {
			m = new Mongo("localhost", 27017 );
			m.setWriteConcern(WriteConcern.SAFE);
			DB db = m.getDB("twitter");
//			DBCollection coll = db.getCollection("tweets");
//			
//			UserTimelineFetcher f = new UserTimelineFetcher("ShubhamGupta", authenticatedTwitter);
//			f.fetchAndStoreInDB(coll);
			
			// Testing updating of fields
			DBCollection apps = db.getCollection("applications");
			BasicDBObject query = new BasicDBObject();
	        query.put("user_id", "deleteme");
			DBObject obj = apps.findOne(query);
			
			long l = Date.parse(obj.get("lastUpdated").toString());
			System.out.println(l);
			Date d = new Date(l);
			System.out.println(d.toGMTString());
			
			System.out.println(TimeUnit.SECONDS.toMillis(3L));
//			Date now = new Date();
//			obj.put("lastUpdated", now);
//			apps.save(obj);
			// System.out.println(obj);
			
			m.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
