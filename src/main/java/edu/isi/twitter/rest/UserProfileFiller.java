package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler;

public class UserProfileFiller implements Runnable {

	public void run() {
		try {
			Mongo m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterMongoDBHandler.DB_NAME);
			DBCollection usersFromTweetMentionsColl = twitterDb
					.getCollection("usersFromTweetMentions");

			DBObject query = new BasicDBObject("incomplete", 1);
			DBCursor cursor = usersFromTweetMentionsColl.find(query);

			Twitter unauthenticatedTwitter = new TwitterFactory().getInstance();
			
			int counter = 0;
			long[] userIds = new long[100];
			while (cursor.hasNext()) {
				DBObject user = cursor.next();
				Double d = Double.parseDouble(user.get("uid").toString());
				long uid = d.longValue();
				
				try {
					userIds[counter++] = uid;
					// Send the request for every 100 users
					if (counter == 100 || !cursor.hasNext()) {
						ResponseList<User> userList = unauthenticatedTwitter.lookupUsers(userIds);
						for (User twitterUser : userList) {
							DBObject dbUser = usersFromTweetMentionsColl.findOne(new BasicDBObject("uid", twitterUser.getId()));
							dbUser.put("timezone", twitterUser.getTimeZone());
							dbUser.put("location", twitterUser.getLocation());
							dbUser.put("created", twitterUser.getCreatedAt());
							dbUser.put("isGeoEnabled", twitterUser.isGeoEnabled());
							dbUser.removeField("incomplete");
							dbUser.put("follow", 0);
							usersFromTweetMentionsColl.save(dbUser);
						}
						System.out.println("Sleeping");
						Thread.sleep(TimeUnit.SECONDS.toMillis(1L));
						System.out.println("Waking");
						counter = 0;
					}
				} catch (TwitterException e) {
					if(e.exceededRateLimitation()) {
						try {
							Thread.sleep(e.getRetryAfter());
							continue;
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					}
					System.out.println("Problem occured with user: " + user.get("name"));
					e.printStackTrace();
					continue;
				} catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
			}

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Thread t = new Thread(new UserProfileFiller());
		t.start();
		
	}

}
