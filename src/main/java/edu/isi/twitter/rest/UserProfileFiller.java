package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;

public class UserProfileFiller implements Runnable {
	ConfigurationBuilder cb;
	
	private static Logger logger = LoggerFactory.getLogger(UserProfileFiller.class);
	
	public UserProfileFiller(ConfigurationBuilder cb) {
		this.cb = cb;
	}

	public void run() {
		try {
			Mongo m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
			DBCollection usersFromTweetMentionsColl = twitterDb.getCollection(TwitterCollections.usersFromTweetMentions.name());
			DBObject query = new BasicDBObject("incomplete", 1);
			Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
			
			while (true) {
				DBCursor cursor = usersFromTweetMentionsColl.find(query);
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
							ResponseList<User> userList = authenticatedTwitter.lookupUsers(userIds);
							for (User twitterUser : userList) {
								DBObject dbUser = usersFromTweetMentionsColl.findOne(new BasicDBObject("uid", twitterUser.getId()));
								if (dbUser != null) {
									dbUser.put("timezone", twitterUser.getTimeZone());
									dbUser.put("location", twitterUser.getLocation());
									dbUser.put("created", twitterUser.getCreatedAt());
									dbUser.put("isGeoEnabled", twitterUser.isGeoEnabled());
									dbUser.removeField("incomplete");
									dbUser.put("follow", 0);
									usersFromTweetMentionsColl.save(dbUser);
									logger.debug("User profile completed for " + twitterUser.getScreenName());
								} else
									logger.error("User not found!" + twitterUser.getName());
								
							}
//							System.out.println("Sleeping");
							Thread.sleep(TimeUnit.SECONDS.toMillis(8L));
//							System.out.println("Waking");
							counter = 0;
						}
					} catch (TwitterException e) {
						if(e.exceededRateLimitation()) {
							try {
								Thread.sleep(TimeUnit.SECONDS.toMillis(e.getRetryAfter()));
								continue;
							} catch (InterruptedException e1) {
								logger.error("InterruptedException", e1);
							}
						}
						logger.error("Problem occured with user: " + user.get("name"), e);
						continue;
					} catch (InterruptedException e) {
						logger.error("InterruptedException", e);
						continue;
					} catch (MongoException e) {
						logger.error("Mongo Exception", e);
						continue;
					}
				}
				// Making the thread sleep for some time before trying again
				try {
					Thread.sleep(TimeUnit.SECONDS.toMillis(30L));
				} catch (InterruptedException e) {
					logger.error("InterruptedException", e);
				}
			}
		} catch (UnknownHostException e) {
			logger.error("UnknownHostException", e);
		} catch (MongoException e) {
			logger.error("Mongo Exception", e);
		}
	}
}
