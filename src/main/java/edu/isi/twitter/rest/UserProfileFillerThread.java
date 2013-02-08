package edu.isi.twitter.rest;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
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
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.usersWaitingList_SCHEMA;
import edu.isi.twitter.AppConfig;

public class UserProfileFillerThread implements Runnable {
	private ConfigurationBuilder cb;
	private AppConfig appConfig;
	
	private static Logger logger = LoggerFactory.getLogger(UserProfileFillerThread.class);
	
	public UserProfileFillerThread(ConfigurationBuilder cb, AppConfig appConfig) {
		this.cb = cb;
		this.appConfig = appConfig;
	}

	public void run() {
		try {
			Mongo m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
			DB twitterDb = m.getDB(appConfig.getDBName());
			DBCollection usersWaitingListColl = twitterDb.getCollection(TwitterCollections.usersWaitingList.name());
			DBObject query = new BasicDBObject(usersWaitingList_SCHEMA.parsed.name(), false);
			Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
			
			while (true) {
				DBCursor cursor = usersWaitingListColl.find(query).snapshot();
				
				/******************* NEEDS TO BE TAKEN CARE OF LATER ***/
				if (cursor.size() == 0)
					break;
				/*******************/
				
				List<Long> userIdsList = new ArrayList<Long>();
				
				while (cursor.hasNext()) {
					DBObject user = cursor.next();
					Double d = Double.parseDouble(user.get(usersWaitingList_SCHEMA.uid.name()).toString());
					long uid = d.longValue();
					userIdsList.add(new Long(uid));
					
					// Sanity check: This case should not happen as we clear the list in case of "continue" cases. This helps avoid thread getting stopped.
					if (userIdsList.size() > 100)
						userIdsList.clear();
					
					// Send the request for every 100 users
					if (userIdsList.size() == 100 || !cursor.hasNext()) {
						ResponseList<User> userList = null;
						long[] userIds = new long[100];
						
						try {
							for (int i=0; i<userIdsList.size(); i++)
								userIds[i] = userIdsList.get(i).longValue();
							userList = authenticatedTwitter.lookupUsers(userIds);
						}  catch (TwitterException e) {
							// Taking care of the rate limiting
							if (e.exceededRateLimitation() || (e.getRateLimitStatus() != null && e.getRateLimitStatus().getRemainingHits() == 0)) {
								long timeToSleep = TimeUnit.MINUTES.toMillis(60); // Default sleep length = 60 minutes
								if (e.getRateLimitStatus().getSecondsUntilReset() > 0) {
									timeToSleep = TimeUnit.SECONDS.toMillis(e.getRateLimitStatus().getSecondsUntilReset() + 60);
								}
								try {
									logger.info("Reached rate limit!", e);
									logger.info("Making user profile thread sleep for " + TimeUnit.MILLISECONDS.toMinutes(timeToSleep) + " minutes.");
									Thread.sleep(timeToSleep);
									logger.info("Waking up the user profile thread!");
									
									// Try again
									userList = authenticatedTwitter.lookupUsers(userIds);
								} catch (InterruptedException e1) {
									logger.error("InterruptedException", e1);
								} catch (TwitterException e1) {
									logger.error("Error getting profiles after waking up the thread.", e1);
									userIdsList.clear();
									continue;
								}
							} else {
								logger.error("Error occured while getting user profiles.", e);
								userIdsList.clear();
								continue;
							}
						}
						if(userList == null) {
							userIdsList.clear();
							continue;
						}
							
						
						/** Save each mentioned user **/
						List<Long> completedUsers = new ArrayList<Long>();
						for (User twitterUser : userList) {
							DBObject dbUser = usersWaitingListColl.findOne(new BasicDBObject(usersWaitingList_SCHEMA.uid.name(), twitterUser.getId()));
							if (dbUser != null) {
								dbUser.put(usersWaitingList_SCHEMA.name.name(), twitterUser.getScreenName());
								dbUser.put(usersWaitingList_SCHEMA.timezone.name(), twitterUser.getTimeZone());
								dbUser.put(usersWaitingList_SCHEMA.location.name(), twitterUser.getLocation());
								dbUser.put(usersWaitingList_SCHEMA.created.name(), twitterUser.getCreatedAt());
								dbUser.put(usersWaitingList_SCHEMA.isGeoEnabled.name(), twitterUser.isGeoEnabled());
								dbUser.put(usersWaitingList_SCHEMA.parsed.name(), true);
								dbUser.put(usersWaitingList_SCHEMA.profileCompleteAndChecked.name(), false);
								try {
									usersWaitingListColl.save(dbUser);
									completedUsers.add(new Long(twitterUser.getId()));
								} catch (MongoException e) {
									logger.error("Error saving profile in users waiting list.", e);
									continue;
								}
							} else
								logger.error("User not found!" + twitterUser.getName());
						}
						
						/** Deleted/unknown/suspended users are also marked as parsed to true but with an additional flag **/
						userIdsList.removeAll(completedUsers);
						for (Long id: userIdsList) {
							DBObject dbUser = usersWaitingListColl.findOne(new BasicDBObject(usersWaitingList_SCHEMA.uid.name(), id));
							if (dbUser != null) {
								dbUser.put(usersWaitingList_SCHEMA.parsed.name(), true);
								dbUser.put(usersWaitingList_SCHEMA.missingProfile.name(), true);
								try {
									usersWaitingListColl.save(dbUser);
								} catch (MongoException e) {
									logger.error("Error saving profile in users waiting list.", e);
									continue;
								}
							}
						}
						
						// Clear the list
						userIdsList.clear();
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
