package edu.isi.twitter.rest;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.UserMentionEntity;
import twitter4j.json.DataObjectFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class UserTimelineFetcher {

	public static int PAGING_SIZE = 200;
	private long uid;
	private Twitter authenticatedTwitter;
	
	private static Logger logger = LoggerFactory.getLogger(UserTimelineFetcher.class);
	
	public UserTimelineFetcher(long uid, Twitter authenticatedTwitter) {
		this.uid = uid;
		this.authenticatedTwitter = authenticatedTwitter;
	}
	
	public boolean fetchAndStoreInDB(DBCollection mdbCollection, DBCollection userColl, DBCollection usersFromTweetMentionsColl, boolean retryAfterRateLimitExceeded) {
		
		Paging paging = new Paging(1, PAGING_SIZE);
		try {
			ResponseList<Status> statuses = authenticatedTwitter.getUserTimeline(uid, paging);
			logger.debug("Initial size of user's timeline history:" + statuses.size());
			doloop:
			do {
				/** Sleeping the thread for some time to avoid generating too many requests very fast. Should not be greater than 3600/350 ~= 10sec **/
				Thread.sleep(TimeUnit.SECONDS.toMillis(2 + (int)Math.random() * ((5-2)+1)));
				
				long lastLongId = 0L; 
				for (int i=0; i<statuses.size(); i++) {
					Status status = statuses.get(i);
					
					/*** Store the tweet into the database ***/
					String json = DataObjectFactory.getRawJSON(status);
					DBObject dbObject = (DBObject)JSON.parse(json);
					if(dbObject != null) {
						try {
							dbObject.put("tweetCreatedAt", status.getCreatedAt());
							mdbCollection.insert(dbObject);
							UserMentionEntity[] mentionEntities = status.getUserMentionEntities();

							if(mentionEntities != null)
								addToUsersCollection(mentionEntities, userColl, usersFromTweetMentionsColl);
						} catch (MongoException e) {
							logger.error("Mongo Exception", e);
							/** Break out of the outer loop as this tweet and all tweets older than this already exists.
							 * We keep looping though in case this is a case where we had to restart because of the rate limit exceeding exception. **/
							if(e.getCode() == 11000 && !retryAfterRateLimitExceeded) {
								break doloop;
							}
						}
					}
					else
						logger.error("Null db object for uid: " + uid);
					
					if(i == statuses.size()-1) {
						lastLongId = status.getId();
						paging.setMaxId(lastLongId-1);
					}
				}
			} while ((statuses = authenticatedTwitter.getUserTimeline(uid, paging)).size() != 0);
			
			return true;
		} catch (TwitterException e1) {
			if(e1.isErrorMessageAvailable())
				logger.error("Error message from API: " + e1.getErrorMessage());
			/** If we exceed the rate limitation **/
			if(e1.exceededRateLimitation()) {
				try {
					logger.error("Rate limit exceeded Going to sleep for " + e1.getRetryAfter());
					// Sleep
					Thread.sleep(TimeUnit.SECONDS.toMillis(e1.getRetryAfter()));
					// Try again after waking up
					fetchAndStoreInDB(mdbCollection, userColl, usersFromTweetMentionsColl, true);
				} catch (InterruptedException e) {
					logger.error("InterruptedException", e);
				}
			} else {
				logger.error("Twitter exception!" , e1);
			}
		} catch (InterruptedException e1) {
			logger.error("Something bad happened to the thread. This should be rare!", e1);
		};
		return true;
	}

	private void addToUsersCollection(UserMentionEntity[] mentionEntities, DBCollection userColl, DBCollection usersFromTweetMentionsColl) {
		for (UserMentionEntity userMention : mentionEntities) {
			try {
				long uid = userMention.getId();
				// Add the user if he does not exists
				if(userColl.find(new BasicDBObject("uid", uid)).count() == 0 && usersFromTweetMentionsColl.find(new BasicDBObject("uid", uid)).count() == 0) {
					logger.debug("Adding user from tweet mentions: " + userMention.getScreenName());
					BasicDBObject userObj = new BasicDBObject();
					userObj.put("uid", new Long(uid).doubleValue());
					userObj.put("name", userMention.getScreenName());
					userObj.put("addedFromTweetMentions", true);
					userObj.put("incomplete", 1);
					usersFromTweetMentionsColl.save(userObj);
				}
			} catch (Exception e) {
				logger.error("Error occured while adding tweet mention user: " + userMention.getName(), e);
				continue;
			}
		}
	}
}