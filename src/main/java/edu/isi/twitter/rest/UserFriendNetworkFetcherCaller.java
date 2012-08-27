package edu.isi.twitter.rest;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;

public class UserFriendNetworkFetcherCaller {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Mongo m;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
			DB db = m.getDB("twitter");
			DBCollection coll = db.getCollection("tweets");
			
//			UserFriendNetworkFetcher f = new UserFriendNetworkFetcher("funnyhumour");
//			f.fetchAndStoreInDB(coll);
			m.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}

	}

}
