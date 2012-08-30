package edu.isi.twitter;

import java.net.UnknownHostException;

import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.twitter.JobManager.TwitterAccountKeys;
import edu.isi.twitter.rest.UsersHistoricalTweetsFetcherThread;

public class SingleUserTest {
	
	public static void main(String[] args) {
		
		try {
			Mongo m = new Mongo("localhost", 27017 );
			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
			
			/** Get the applications access tokens and keys information **/
			DBCollection appsColl = twitterDb.getCollection("applications");
			long numberOfApps = 1L;	///////// WORKING WITH JUST 1 APPLCIATION
			DBCursor appsCursor = appsColl.find();
			
			/** Calculating the number and range of users to be handled by each application thread **/
			DBCollection userColl = twitterDb.getCollection("SingleUserTest");
			long totallUsers = userColl.getCount();
			long interval = totallUsers/numberOfApps;
			
			System.out.println("Total users: " + totallUsers);
			System.out.println("Interval: " + interval);
			
			for(int i=0; i<numberOfApps; i++) {
				
				/*** Create the configuration builder object for each application ***/
				DBObject appObj = appsCursor.next();
				ConfigurationBuilder cb = new ConfigurationBuilder()
			    	.setOAuthConsumerKey(appObj.get(TwitterAccountKeys.consumer_key.name()).toString())
			    	.setOAuthConsumerSecret(appObj.get(TwitterAccountKeys.consumer_key_secret.name()).toString())
			    	.setOAuthAccessToken(appObj.get(TwitterAccountKeys.access_token.name()).toString())
			    	.setOAuthAccessTokenSecret(appObj.get(TwitterAccountKeys.access_token_secret.name()).toString())
			    	.setJSONStoreEnabled(true);
				
				/*** Get the start index and end index for each application thread ***/
				int startIndex = new Long(i*interval).intValue();
		        int endIndex = (i != numberOfApps-1) ? new Long(i*interval + interval -1).intValue() : new Long(userColl.count()-1).intValue();
		        
		        Thread t = new Thread(new UsersHistoricalTweetsFetcherThread(startIndex, endIndex, cb));
		        t.start();
		        System.out.println("Application # " + i);
		        System.out.println("Start UID: " + startIndex);
		        System.out.println("End UID: " + endIndex);
		        
		        //////////////// BREAKING AFTER 1 APPLICATION ONLY ////////////
		        break;
			}
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	private static long getUIDAtInterval(DBCollection coll, int offset) {
		System.out.println("Getting UID at interval: " + offset);
		long uid = 0L;
		DBCursor cursor = coll.find().skip(offset).limit(1);
        while(cursor.hasNext()) {
        	DBObject obj = cursor.next();
        	if(obj.containsField("uid")) {
        		System.out.println(obj.get("uid"));
        		System.out.println(obj.get("uid").getClass());
        		Double d = Double.parseDouble(obj.get("uid").toString());
            	uid = d.longValue();
        	}
        }
        cursor.close();
        return uid;
	}

}
