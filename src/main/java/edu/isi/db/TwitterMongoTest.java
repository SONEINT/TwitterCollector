package edu.isi.db;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class TwitterMongoTest {

	public static void main(String[] args) throws UnknownHostException, MongoException {
		Mongo m = MongoDBHandler.getNewMongoConnection();
		DB db = m.getDB("twitter");
		
		DBCollection coll = db.getCollection("tweets");
		
		String json = "{'database' : 'mkyongDB','table' : 'hosting'," +
				"'detail' : {'records' : 99, 'index' : 'vps_index1', 'active' : 'true'}}}";
		 
		DBObject dbObject = (DBObject)JSON.parse(json);
		coll.insert(dbObject);
	}

}
