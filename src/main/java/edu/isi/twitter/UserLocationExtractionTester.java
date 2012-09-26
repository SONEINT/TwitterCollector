package edu.isi.twitter;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.users_SCHEMA;

public class UserLocationExtractionTester {

	private static Logger logger = LoggerFactory.getLogger(UserLocationExtractionTester.class);
	
	public static void main(String[] args) {
		// Setup mongodb
		Mongo m = null;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
		} catch (UnknownHostException e) {
			logger.error("UnknownHostException", e);
		} catch (MongoException e) {
			logger.error("MongoException", e);
		}
		if(m == null) {
			logger.error("Error getting connection to MongoDB! Cannot proceed with this thread.");
			return;
		}
		DB twitterDb = m.getDB(TwitterApplication.twitter.name());
		DBCollection usersColl = twitterDb.getCollection(TwitterCollections.users.name());
		
		int tzCounter = 0;
		int usersCounter = 0;
		
		DBCursor usersC = usersColl.find();
		while (usersC.hasNext()) {
			usersCounter++;
			if (usersCounter%50000 == 0 )
				System.out.println("Done with " + usersCounter + "users");
			
			DBObject user = usersC.next();
			String location = user.get(users_SCHEMA.location.name()).toString();
			String timezone = user.get(users_SCHEMA.timezone.name()).toString();
			
			UserLocationIdentifier ex = new UserLocationIdentifier(location, timezone);
			ex.isLocationInMiddleEast();
		}
		
		System.out.println("# Users with middle east timezones: " + tzCounter);
		m.close();
	}

}
