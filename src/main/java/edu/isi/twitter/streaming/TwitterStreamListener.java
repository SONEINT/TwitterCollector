package edu.isi.twitter.streaming;

import java.net.UnknownHostException;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.UserMentionEntity;
import twitter4j.json.DataObjectFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TWEET_SOURCE;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.USER_SOURCE;
import edu.isi.db.TwitterMongoDBHandler.tweets_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.usersWaitingList_SCHEMA;
import edu.isi.statistics.StatisticsManager;
import edu.isi.twitter.AppConfig;


public class TwitterStreamListener implements StatusListener {
	private Mongo m;
	private DB twitterDb;
	private DBCollection tweetCollection;
	private DBCollection usersWaitingListColl;
	private DBCollection tweetsLogColl;
	private DBCollection replyToColl;
	private DBCollection mentionsColl;
	private AppConfig appConfig;
	private StatisticsManager statsMgr;
	
	private static Logger logger = LoggerFactory.getLogger(TwitterStreamListener.class);
	
	public TwitterStreamListener(AppConfig appConfig, StatisticsManager statsMgr) throws UnknownHostException, MongoException {
		this.appConfig = appConfig;
		this.statsMgr = statsMgr;
		
		m = MongoDBHandler.getNewMongoConnection();
		m.setWriteConcern(WriteConcern.SAFE);
		twitterDb = m.getDB(this.appConfig.getDBName());
		tweetCollection = twitterDb.getCollection(TwitterCollections.tweets.name());
		usersWaitingListColl = twitterDb.getCollection(TwitterCollections.usersWaitingList.name());
		tweetsLogColl = twitterDb.getCollection(TwitterCollections.tweetsLog.name());
		replyToColl = twitterDb.getCollection(TwitterCollections.replyToTable.name());
		mentionsColl = twitterDb.getCollection(TwitterCollections.mentionsTable.name());
	}
	
	public void onStatus(Status status) {
		try {
			/*** Store the tweet into the database ***/
			String json = DataObjectFactory.getRawJSON(status);
			DBObject dbObject = (DBObject)JSON.parse(json);
			dbObject.put(tweets_SCHEMA.tweetCreatedAt.name(), status.getCreatedAt());
			dbObject.put(tweets_SCHEMA.APISource.name(), TWEET_SOURCE.Streaming.name());
			tweetCollection.insert(dbObject);
			
			DateTime now = new DateTime();
			// Add this tweet's information in the tweetsLog collection
			TwitterMongoDBHandler.addToTweetLogTable(tweetsLogColl, TWEET_SOURCE.Streaming.name(), 
					status.getId(), now.getMillis(), status.getCreatedAt().getTime());
			
			// Add to replyTo table is the tweet was posted in reply to some previous tweet
			if (status.getInReplyToStatusId() != -1) {
				TwitterMongoDBHandler.addToReplyToTable(replyToColl, status.getId(), status.getInReplyToStatusId()
						, status.getUser().getId(), status.getInReplyToUserId(), now.getMillis(), status.getCreatedAt().getTime());
			}
			
			// logger.debug("Received tweet from stream: " + status.getText());
			/*** Check if any user was mentioned. If yes, then check if his user id needs to saved in the users table ***/
			UserMentionEntity[] mentionedEntities = status.getUserMentionEntities();
			if(mentionedEntities != null && mentionedEntities.length != 0) {
				if (appConfig.isFollowMentions()) {
					addUsersToUsersWaitingListAndMentionsCollection(mentionedEntities, status);
				} else {
					for (UserMentionEntity userMention : mentionedEntities) {
						TwitterMongoDBHandler.addToMentionsTable(mentionsColl, status.getId(), status.getUser().getId(), 
								userMention.getId(), now.getMillis(), status.getCreatedAt().getTime());
					}
				}
			}
			
			// Increment the counter for streaming in the Statistics Manager
			statsMgr.incrementStreamingTweetCounter();
			
		} catch (MongoException e) {
			logger.debug("Mongo Exception!", e);
		}	
    }

    
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        logger.debug("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }

    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
    	logger.info("Got track limitation notice:" + numberOfLimitedStatuses);
    }

    public void onScrubGeo(long userId, long upToStatusId) {
    	logger.info("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }

    public void onException(Exception ex) {
    	logger.error("Exception from streaming!", ex);
    }
    
    private void addUsersToUsersWaitingListAndMentionsCollection(UserMentionEntity[] mentionEntities, Status status) {
		for (UserMentionEntity userMention : mentionEntities) {
			try {
				// Add to the mentions table
				TwitterMongoDBHandler.addToMentionsTable(mentionsColl, status.getId(), status.getUser().getId(), 
						userMention.getId(), new DateTime().getMillis(), status.getCreatedAt().getTime());
				
				// Add to the usersWaitingList collection
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

	@Override
	public void onStallWarning(StallWarning warning) {
		logger.info("Got stall warning: " + warning.getMessage());
		
	}
}
