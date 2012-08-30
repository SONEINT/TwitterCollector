package edu.isi.db;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class TwitterMongoTest {

	public static void main(String[] args) throws UnknownHostException, MongoException {
		Mongo m = new Mongo("localhost", 27017 );
		DB db = m.getDB("twitter");
		DBCollection coll = db.getCollection("users");
		
		long uid = 12;
		
		System.out.println(coll.findOne(new BasicDBObject("uid", uid)).get("name"));
		
//		DBCollection coll = db.getCollection("tweets");
		
//		String json = "{'database' : 'mkyongDB','table' : 'hosting'," +
//				"'detail' : {'records' : 99, 'index' : 'vps_index1', 'active' : 'true'}}}";
//		 
//		DBObject dbObject = (DBObject)JSON.parse(json);
//		coll.insert(dbObject);
	}

}
