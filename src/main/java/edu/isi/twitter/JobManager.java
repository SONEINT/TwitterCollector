package edu.isi.twitter;

import java.net.UnknownHostException;
import java.util.List;

import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler;
import edu.isi.twitter.rest.UsersHistoricalTweetsFetcherThread;

public class JobManager {
	
	public enum TwitterAccountKeys {
		user_id, access_token, access_token_secret, consumer_key, consumer_key_secret
	}
	
	public static void main(String[] args) {
		runTwitterStreamListener();
		try {
			Mongo m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterMongoDBHandler.DB_NAME);
			List<ConfigurationBuilder> allConfigs = TwitterApplicationManager.getAllApplicationConfigurations();
			
			/** Calculating the number and range of users to be handled by each application thread **/
			DBCollection userColl = twitterDb.getCollection("SingleUserTest");
			long totallUsers = userColl.getCount();
			long interval = totallUsers/allConfigs.size();
			
			System.out.println("Total users: " + totallUsers);
			System.out.println("Interval: " + interval);
			
			for(int i=0; i<allConfigs.size(); i++) {
				ConfigurationBuilder config = allConfigs.get(i);
				
				/*** Get the start index and end index for each application thread ***/
				int startIndex = new Long(i*interval).intValue();
		        int endIndex = (i != allConfigs.size()-1) ? new Long(i*interval + interval -1).intValue() : new Long(userColl.count()-1).intValue();
		        
		        Thread t = new Thread(new UsersHistoricalTweetsFetcherThread(startIndex, endIndex, config));
		        t.start();
		        System.out.println("Application # " + i);
		        System.out.println("Start UID: " + startIndex);
		        System.out.println("End UID: " + endIndex);
			}
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	private static void runTwitterStreamListener() {
		
	}

	/*
	private static long getUIDAtInterval(DBCollection coll, int offset) {
		System.out.println("Getting UID at interval: " + offset);
		long uid = 0L;
		DBCursor cursor = coll.find().skip(offset).limit(1);
        while(cursor.hasNext()) {
        	DBObject obj = cursor.next();
        	if(obj.containsField("uid")) {
        		uid = Long.parseLong(obj.get("uid").toString());
        	}
        }
        cursor.close();
        return uid;
	}
	*/
}