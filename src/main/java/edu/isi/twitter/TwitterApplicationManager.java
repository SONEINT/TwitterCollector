package edu.isi.twitter;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.twitter.JobManager.TwitterAccountKeys;

public class TwitterApplicationManager {
	public enum ApplicationTag {
		Streaming, UserTimelineFetcher, UserProfileLookup, UserNetworkGraphFetcher
	}
	
	public static List<ConfigurationBuilder> getAllApplicationConfigurations() {
		List<ConfigurationBuilder> appConfigs = new ArrayList<ConfigurationBuilder>();
		Mongo m = null;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
			
			/** Get the applications access tokens and keys information **/
			DBCollection appsColl = twitterDb.getCollection("applications");
			long numberOfApps = appsColl.count();
			DBCursor appsCursor = appsColl.find();
			
			for(int i=0; i<numberOfApps; i++) {
				/*** Create the configuration builder object for each application ***/
				DBObject appObj = appsCursor.next();
				ConfigurationBuilder cb = buildConfigurationBuilder(appObj);
				appConfigs.add(cb);
			}
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		} finally {
			m.close();
		}
		return appConfigs;
	}
	
	public static ConfigurationBuilder getOneConfigurationBuilderByTag(ApplicationTag tag) {
		return getAllConfigurationBuildersByTag(tag).get(0);
	}
	
	public static List<ConfigurationBuilder> getAllConfigurationBuildersByTag(ApplicationTag tag) {
		List<ConfigurationBuilder> appConfigs = new ArrayList<ConfigurationBuilder>();
		Mongo m = null;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
			DBCollection appsColl = twitterDb.getCollection("applications");
			
			DBCursor appCursor = appsColl.find(new BasicDBObject("tag", tag.name()));
			while(appCursor.hasNext()) {
				appConfigs.add(buildConfigurationBuilder(appCursor.next()));
			}
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		} finally {
			m.close();
		}
		return appConfigs;
	}
	
	private static ConfigurationBuilder buildConfigurationBuilder(DBObject appObj) {
		ConfigurationBuilder cb = new ConfigurationBuilder()
	    	.setOAuthConsumerKey(appObj.get(TwitterAccountKeys.consumer_key.name()).toString())
	    	.setOAuthConsumerSecret(appObj.get(TwitterAccountKeys.consumer_key_secret.name()).toString())
	    	.setOAuthAccessToken(appObj.get(TwitterAccountKeys.access_token.name()).toString())
	    	.setOAuthAccessTokenSecret(appObj.get(TwitterAccountKeys.access_token_secret.name()).toString())
	    	.setJSONStoreEnabled(true);
		return cb;
	}
}
