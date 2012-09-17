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
import com.mongodb.WriteConcern;

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
			m.setWriteConcern(WriteConcern.SAFE);
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
//							logger.info("Sleeping");
							Thread.sleep(TimeUnit.SECONDS.toMillis(3L));
//							System.out.println("Waking");
							counter = 0;
						}
						userIds[counter++] = uid;
					} catch (TwitterException e) {
						// Taking care of the rate limiting
						if (e.exceededRateLimitation() || (e.getRateLimitStatus() != null && e.getRateLimitStatus().getRemainingHits() == 0)) {
							if (e.getRateLimitStatus().getSecondsUntilReset() > 0) {
								try {
									logger.error("Reached rate limit!", e);
									logger.info("Making user profile thread sleep for " + e.getRateLimitStatus().getSecondsUntilReset());
									Thread.sleep(TimeUnit.SECONDS.toMillis(e.getRateLimitStatus().getSecondsUntilReset()));
									logger.info("Waking up the user profile thread!");
									continue;
								} catch (InterruptedException e1) {
									logger.error("InterruptedException", e1);
								}
							} else {
								logger.info("Making user profile thread sleep for 60 minutes");
								try {
									Thread.sleep(TimeUnit.MINUTES.toMillis(60));
									logger.info("Waking up the user profile thread");
									continue;
								} catch (InterruptedException e1) {
									e1.printStackTrace();
								}
							}
						} else
							logger.error("Problem occured while getting user profiles.", e);
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
					logger.info("Making the profile lookup thread sleep before starting the loop again!");
					Thread.sleep(TimeUnit.SECONDS.toMillis(30L));
					logger.info("Waking up the profile lookup thread!");
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
