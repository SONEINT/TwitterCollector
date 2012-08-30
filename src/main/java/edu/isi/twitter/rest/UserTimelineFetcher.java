package edu.isi.twitter.rest;

import java.util.Calendar;
import java.util.Date;
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
	private static int TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS = 14;
	private int numberOfTweetsInLast2Weeks = 0;
	
	public UserTimelineFetcher(long uid, Twitter authenticatedTwitter) {
		this.uid = uid;
		this.authenticatedTwitter = authenticatedTwitter;
	}
	
	public int getNumberOfTweetsInLast2Weeks() {
		return numberOfTweetsInLast2Weeks;
	}

	public boolean fetchAndStoreInDB(DBCollection mdbCollection, DBCollection userColl, DBCollection usersFromTweetMentionsColl, boolean retryAfterRateLimitExceeded) {
		
		Paging paging = new Paging(1, PAGING_SIZE);
		try {
			ResponseList<Status> statuses = authenticatedTwitter.getUserTimeline(uid, paging);
			logger.debug("Initial size of user's timeline history:" + statuses.size());

			// 2 week's before time
			Calendar date = Calendar.getInstance();
			date.add(Calendar.HOUR, -(24* TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS));
			Date twoWeeksAgo = date.getTime();
			
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
						
						// Increase the numberOfTweetsInLast2Weeks counter if the tweet was created in last 2 weeks
						Date createdAt = status.getCreatedAt();
						if(createdAt.after(twoWeeksAgo))
							numberOfTweetsInLast2Weeks++;
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
		} catch (TwitterException e) {
			// Taking care of the rate limiting
			if (e.exceededRateLimitation() || e.getRateLimitStatus().getRemainingHits() == 0) {
				if (e.getRateLimitStatus().getSecondsUntilReset() != 0) {
					try {
						logger.error("Reached rate limit!", e);
						logger.info("Making timeline fetcher thread sleep for " + e.getRateLimitStatus().getSecondsUntilReset());
						Thread.sleep(TimeUnit.SECONDS.toMillis(e.getRateLimitStatus().getSecondsUntilReset()));
						logger.info("Waking up the timeline fetcher thread!");
						// Try again after waking up
						fetchAndStoreInDB(mdbCollection, userColl, usersFromTweetMentionsColl, true);
					} catch (InterruptedException e1) {
						logger.error("InterruptedException", e1);
					}
				}
			} else
				logger.error("Problem occured with user: " + uid, e);
		} catch (InterruptedException e1) {
			logger.error("Something bad happened to the thread. This should be rare!", e1);
		};
		return true;
	}

	private void addToUsersCollection(UserMentionEntity[] mentionEntities, DBCollection userColl, DBCollection usersFromTweetMentionsColl) {
		for (UserMentionEntity userMention : mentionEntities) {
			try {
				long uid = userMention.getId();
				logger.debug("Adding user from tweet mentions: " + userMention.getScreenName());
				BasicDBObject userObj = new BasicDBObject();
				userObj.put("uid", new Long(uid).doubleValue());
				userObj.put("name", userMention.getScreenName());
				userObj.put("addedFromTweetMentions", true);
				userObj.put("incomplete", 1);
				usersFromTweetMentionsColl.save(userObj);
			} catch (MongoException e) {
				if (e.getCode() == 11000)
					logger.error("Tweet mention user already exists: " + userMention.getName() + ". Error: " + e.getMessage());
				continue;
			} catch (Exception e) {
				logger.error("Error occured while adding tweet mention user: " + userMention.getName(), e);
				continue;
			}
		}
	}
}