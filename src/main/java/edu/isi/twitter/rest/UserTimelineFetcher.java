package edu.isi.twitter.rest;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
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

import edu.isi.db.TwitterMongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TWEET_SOURCE;
import edu.isi.db.TwitterMongoDBHandler.USER_SOURCE;
import edu.isi.db.TwitterMongoDBHandler.tweets_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.usersWaitingList_SCHEMA;

public class UserTimelineFetcher {

	public static int PAGING_SIZE = 200;
	private long uid;
	private Twitter authenticatedTwitter;
	private boolean followMentions;
	
	private static Logger logger = LoggerFactory.getLogger(UserTimelineFetcher.class);
	private static int TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS = 14;
	private int numberOfTweetsInLast2Weeks = 0;
	
	public UserTimelineFetcher(long uid, Twitter authenticatedTwitter, boolean followMentions) {
		this.uid = uid;
		this.authenticatedTwitter = authenticatedTwitter;
		this.followMentions = followMentions;
	}
	
	public int getNumberOfTweetsInLast2Weeks() {
		return numberOfTweetsInLast2Weeks;
	}

	public boolean fetchAndStoreInDB(DBCollection tweetsCollection, DBCollection userColl, 
			DBCollection usersWaitingListColl, DBCollection currentThreadsColl, DBObject threadObj, 
			DBCollection tweetsLogColl, DBCollection replyToColl, DBCollection mentionsColl) {
		
		Paging paging = new Paging(1, PAGING_SIZE);

		// 2 week's before time
		Calendar date = Calendar.getInstance();
		date.add(Calendar.HOUR, -(24* TWEETS_FREQUENCY_CALCULATION_DURATION_DAYS));
		Date twoWeeksAgo = date.getTime();
		
		while (true) {
			ResponseList<Status> statuses = null;
			try {
				statuses = authenticatedTwitter.getUserTimeline(uid, paging);
			} catch (TwitterException e) {
				// Taking care of the rate limiting
				if (e.exceededRateLimitation() || (e.getRateLimitStatus() != null && e.getRateLimitStatus().getRemainingHits() == 0)) {
					logger.info("Reached rate limit!");
					long timeToSleep = TimeUnit.MINUTES.toMillis(60); // Default sleep length = 60 minutes
					if (e.getRateLimitStatus().getSecondsUntilReset() > 0) {
						timeToSleep = TimeUnit.SECONDS.toMillis(e.getRateLimitStatus().getSecondsUntilReset() + 60);
					}
					try {
//						logger.error("Rate limit error: ", e);
						logger.info("Reached rate limit! Making timeline fetcher thread sleep for " + TimeUnit.MILLISECONDS.toMinutes(timeToSleep) + " minutes.");
						
						threadObj.put("status", "sleeping");
						currentThreadsColl.save(threadObj);
						try {
							Thread.sleep(timeToSleep);
						} catch (InterruptedException e1) {
							logger.error("InterruptedException", e1);
						}
						threadObj.put("status", "notSleeping");
						currentThreadsColl.save(threadObj);
	
						logger.info("Waking up the timeline fetcher thread!");
						// Try again after waking up
						statuses = authenticatedTwitter.getUserTimeline(uid, paging);
					} catch (TwitterException e1) {
						logger.error("Error getting tweets after waking up the thread!", e1);
					}
				} else
					logger.error("Problem occured with user: " + uid + ". Error message: " + e.getMessage());
			}
			if (statuses == null || statuses.size() == 0)
				break;
			
			long lastLongId = 0L; 
			for (int i=0; i<statuses.size(); i++) {
				Status status = statuses.get(i);
				
				/*** Store the tweet into the database ***/
				String json = DataObjectFactory.getRawJSON(status);
				DBObject dbObject = (DBObject)JSON.parse(json);
				if(dbObject != null) {
					try {
						dbObject.put(tweets_SCHEMA.tweetCreatedAt.name(), status.getCreatedAt());
						dbObject.put(tweets_SCHEMA.APISource.name(), TWEET_SOURCE.Timeline.name());
						tweetsCollection.insert(dbObject);
						
						DateTime now = new DateTime();
						// Add this tweet's information in the tweetsLog collection
						TwitterMongoDBHandler.addToTweetLogTable(tweetsLogColl, TWEET_SOURCE.Timeline.name(), 
								status.getId(), now.getMillis(), status.getCreatedAt().getTime());
						
						// Add to replyTo table is the tweet was posted in reply to some previous tweet
						if (status.getInReplyToStatusId() != -1) {
							TwitterMongoDBHandler.addToReplyToTable(replyToColl, status.getId(), status.getInReplyToStatusId()
									, status.getUser().getId(), status.getInReplyToUserId(), now.getMillis(), status.getCreatedAt().getTime());
						}
						
						// Add to userWaitingList collection if required
						UserMentionEntity[] mentionedEntities = status.getUserMentionEntities();
						if (mentionedEntities != null && mentionedEntities.length != 0) {
							if (followMentions) {
								addUsersToUsersWaitingListAndMentionsCollection(mentionedEntities, usersWaitingListColl, mentionsColl, status);
							} else {
								for (UserMentionEntity userMention : mentionedEntities) {
									TwitterMongoDBHandler.addToMentionsTable(mentionsColl, status.getId(), status.getUser().getId(), 
											userMention.getId(), now.getMillis(), status.getCreatedAt().getTime());
								}
							}
						}
					} catch (MongoException e) {
						if(e.getCode() == 11000) {
							// do nothing
						}
						else {
							logger.error("Mongo Exception: " + e.getMessage());
						}
					}
					
					// Increase the numberOfTweetsInLast2Weeks counter if the tweet was created in last 2 weeks
					Date createdAt = status.getCreatedAt();
					if(createdAt.after(twoWeeksAgo))
						numberOfTweetsInLast2Weeks++;
				}
				else {
					logger.error("Null db object for uid: " + uid);
				}
				
				if(i == statuses.size()-1) {
					lastLongId = status.getId();
					paging.setMaxId(lastLongId-1);
				}
			}
		}
		return true;
	}

	private void addUsersToUsersWaitingListAndMentionsCollection(UserMentionEntity[] mentionEntities, DBCollection usersWaitingListColl, DBCollection mentionsColl, Status status) {
		for (UserMentionEntity userMention : mentionEntities) {
			try {
				// Add to the mentions table
				TwitterMongoDBHandler.addToMentionsTable(mentionsColl, status.getId(), status.getUser().getId(), 
						userMention.getId(), new DateTime().getMillis(), status.getCreatedAt().getTime());
				
				BasicDBObject userObj = new BasicDBObject(usersWaitingList_SCHEMA.uid.name(), new Long(userMention.getId()).doubleValue());
				userObj.put(usersWaitingList_SCHEMA.name.name(), userMention.getScreenName());
				userObj.put(usersWaitingList_SCHEMA.source.name(), USER_SOURCE.Mentions.name());
				userObj.put(usersWaitingList_SCHEMA.friendDepth.name(), 0);
				userObj.put(usersWaitingList_SCHEMA.followerDepth.name(), 0);
				userObj.put(usersWaitingList_SCHEMA.followMentions.name(), false);
				userObj.put(usersWaitingList_SCHEMA.parsed.name(), false);
				
				usersWaitingListColl.insert(userObj);
			} catch (MongoException e) {
				if (e.getCode() == 11000)
					//logger.debug("Tweet mention user already exists: " + userMention.getName() + ". Error: " + e.getMessage());
					continue;
			} catch (Exception e) {
				logger.error("Error occured while adding tweet mention user: " + userMention.getName(), e);
				continue;
			}
		}
	}
}