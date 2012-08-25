package edu.isi.twitter.rest;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler;

public class UsersFriendsAndFollowersManager implements Runnable {

	public void run() {
		Mongo m;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterMongoDBHandler.DB_NAME);
			DBCollection usersGraphListColl = twitterDb.getCollection("usersGraphListTest");
			DBCollection usersGraphColl = twitterDb.getCollection("usersGraph");
			DBCollection usersGraphActionListColl = twitterDb.getCollection("usersGraphActionList");
			
			DBCursor cursor = usersGraphListColl.find();
			while (cursor.hasNext()) {
				DBObject user = cursor.next();
				UserFriendNetworkFetcher f = new UserFriendNetworkFetcher(user.get("name").toString());
				f.fetchAndStoreInDB(usersGraphColl, usersGraphActionListColl);
			}
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	
	public static void main (String[] args) {
		Thread t = new Thread(new UsersFriendsAndFollowersManager());
		t.start();
	}
}
