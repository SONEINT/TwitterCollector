package edu.isi.twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import edu.isi.db.TwitterMongoDBHandler.users_SCHEMA;

public class UserLocationExtractionTester {

	private static Logger logger = LoggerFactory.getLogger(UserLocationExtractionTester.class);
	
	public static void main(String[] args) {
		// Create the gazeteer index
		GazetteerLuceneManager gzMgr = new GazetteerLuceneManager();
		try {
//			gzMgr.createIndexFromGazetteerCSV(new File("data/test.csv"), true);
			gzMgr.createIndexFromGazetteerCSV(new File("data/middle-east-gazatteer-v2.csv"), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
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
//		DBCollection usersColl = twitterDb.getCollection(TwitterCollections.users.name());
		DBCollection usersColl = twitterDb.getCollection("usersTest");
		
		int tzCounter = 0;
		int usersCounter = 0;
		
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("output.txt"));
			BufferedWriter out2 = new BufferedWriter(new FileWriter("MiddleEastUsersList.txt"));
			DBCursor usersC = usersColl.find();
			while (usersC.hasNext()) {
				usersCounter++;
				if (usersCounter%10000 == 0 )
					System.out.println("Done with " + usersCounter + "users");
				
				DBObject user = usersC.next();
				String location = user.get(users_SCHEMA.location.name()).toString();
				String timezone = user.get(users_SCHEMA.timezone.name()).toString();
				
				UserLocationIdentifier ex = new UserLocationIdentifier(location, timezone, gzMgr);
				boolean isMiddleEastUser = ex.isLocatedInMiddleEast();
				if (isMiddleEastUser) {
					user.put("isLocatedInMiddleEast", true);
					out.write("Location: " + user.get("location") + " | Timezone: " + user.get("timezone") + "\n");
					out2.write(user.get("uid") + "\n");
					tzCounter++;
				} else 
					user.put("isLocatedInMiddleEast", false);
				
				usersColl.save(user);
			}
			
			System.out.println("# Users in middle east" + tzCounter);
			m.close();
			out.flush();
			out.close();
			
			out2.flush();
			out2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

}
