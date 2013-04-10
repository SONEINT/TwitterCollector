package edu.isi.twitter;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.applications_SCHEMA;

public class TwitterApplicationManager {
	public enum ApplicationTag {
		Streaming, UserTimelineFetcher, UserProfileLookup, UserNetworkGraphFetcher, Search
	}
	
	public static List<ConfigurationBuilder> getAllApplicationConfigurations(String dBName) {
		List<ConfigurationBuilder> appConfigs = new ArrayList<ConfigurationBuilder>();
		Mongo m = null;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(dBName);
			
			/** Get the applications access tokens and keys information **/
			DBCollection appsColl = twitterDb.getCollection(TwitterCollections.applications.name());
			long numberOfApps = appsColl.count();
			DBCursor appsCursor = appsColl.find().addOption(Bytes.QUERYOPTION_NOTIMEOUT);
			
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
	
	public static ConfigurationBuilder getOneConfigurationBuilderByTag(ApplicationTag tag, String dBName) {
		List<ConfigurationBuilder> configs = getAllConfigurationBuildersByTag(tag, dBName);
		if (configs.isEmpty())
			return null;
		else
			return configs.get(0);
	}
	
	public static ConfigurationBuilder getOneConfigurationBuilderByAppName(String appName, String dBName) {
		Mongo m = null;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(dBName);
			DBCollection appsColl = twitterDb.getCollection(TwitterCollections.applications.name());
			
			DBObject appObj = appsColl.findOne(new BasicDBObject(applications_SCHEMA.user_id.name(), appName));
			if (appObj == null)
				return null;
			else 
				return buildConfigurationBuilder(appObj); 
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		} finally {
			m.close();
		}
		return null;
	}
	
	public static List<ConfigurationBuilder> getAllConfigurationBuildersByTag(ApplicationTag tag, String dBName) {
		List<ConfigurationBuilder> appConfigs = new ArrayList<ConfigurationBuilder>();
		Mongo m = null;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(dBName);
			DBCollection appsColl = twitterDb.getCollection(TwitterCollections.applications.name());
			
			DBCursor appCursor = appsColl.find(new BasicDBObject(applications_SCHEMA.tag.name(), tag.name())).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
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
	    	.setOAuthConsumerKey(appObj.get(applications_SCHEMA.consumer_key.name()).toString())
	    	.setOAuthConsumerSecret(appObj.get(applications_SCHEMA.consumer_key_secret.name()).toString())
	    	.setOAuthAccessToken(appObj.get(applications_SCHEMA.access_token.name()).toString())
	    	.setOAuthAccessTokenSecret(appObj.get(applications_SCHEMA.access_token_secret.name()).toString())
	    	.setJSONStoreEnabled(true)
	    	.setIncludeRTsEnabled(true);
		return cb;
	}
}
