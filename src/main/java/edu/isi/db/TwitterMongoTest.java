package edu.isi.db;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class TwitterMongoTest {

	public static void main(String[] args) throws UnknownHostException, MongoException, InterruptedException {
		
		Set<Long> test = new HashSet<Long>();
		test.add(new Long(0l));
		test.add(new Long(1l));
		test.add(new Long(2l));
		test.add(new Long(2l));
		test.add(new Long(2l));
		System.out.println(test.size());
		System.exit(0);
		
		Mongo m = MongoDBHandler.getNewMongoConnection();
		DB db = m.getDB("twitter");
		DBCollection coll = db.getCollection("test");
		
		coll.insert(new BasicDBObject("time", new Date().getTime()));
		//Thread.sleep(1000);
		coll.insert(new BasicDBObject("time", new Date().getTime()));
		//Thread.sleep(1000);
		coll.insert(new BasicDBObject("time", new Date().getTime()));
		//Thread.sleep(1000);
		coll.insert(new BasicDBObject("time", 0l));
		coll.insert(new BasicDBObject("time", 1l));
		coll.insert(new BasicDBObject("time", 2l));
		
		int TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS = 14;
		
		//float activity = 220/(float)TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS;
		coll.insert(new BasicDBObject("activity", 220/(float)TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS));
		
		System.out.println("Done");
		
//		DBCollection coll = db.getCollection("tweets");
		
//		String json = "{'database' : 'mkyongDB','table' : 'hosting'," +
//				"'detail' : {'records' : 99, 'index' : 'vps_index1', 'active' : 'true'}}}";
//		 
//		DBObject dbObject = (DBObject)JSON.parse(json);
//		coll.insert(dbObject);
	}

}
