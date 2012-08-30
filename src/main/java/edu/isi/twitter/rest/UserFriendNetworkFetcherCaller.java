package edu.isi.twitter.rest;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

public class UserFriendNetworkFetcherCaller {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Mongo m;
		try {
			m = new Mongo("localhost", 27017 );
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
