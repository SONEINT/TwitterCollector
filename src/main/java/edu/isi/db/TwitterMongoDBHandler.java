package edu.isi.db;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class TwitterMongoDBHandler {
	
	public static String DB_NAME = "twitter";
	public static String USERS_TABLE_NAME = "users";
	public static String TWEETS_TABLE_NAME = "tweets";
	
	public long[] getUserIdList() throws UnknownHostException, MongoException {
		Mongo m = MongoDBHandler.getNewMongoConnection();
		DB db = m.getDB(DB_NAME);
		
		BasicDBObject query = new BasicDBObject();
		query.put("follow", 1);
		DBCollection coll = db.getCollection(USERS_TABLE_NAME);
		
		List<Long> userIds = new ArrayList<Long>();
		DBCursor cursor = coll.find(query);
		
        try {
            while(cursor.hasNext()) {
            	DBObject obj = cursor.next();
            	if(obj.containsField("uid")) {
            		userIds.add(Long.parseLong(obj.get("uid").toString()));
            	}
            }
            System.out.println(userIds);
            System.out.println(userIds.size());
        } finally {
            cursor.close();
        }
        
        long[] arr = new long[userIds.size()];
        int i = 0;
        for (Long userId: userIds) {
        	arr[i++] = userId;
        }
        m.close();
        return arr;
	}
}
