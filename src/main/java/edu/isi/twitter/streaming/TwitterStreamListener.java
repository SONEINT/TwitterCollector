package edu.isi.twitter.streaming;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.USER_SOURCE;
import edu.isi.db.TwitterMongoDBHandler.usersWaitingList_SCHEMA;
import edu.isi.twitter.AppConfig;


public class TwitterStreamListener implements StatusListener {
	private Mongo m;
	private DB twitterDb;
	private DBCollection tweetCollection;
	private DBCollection usersWaitingListColl;
	private AppConfig appConfig;
	
	private static Logger logger = LoggerFactory.getLogger(TwitterStreamListener.class);
	
	public TwitterStreamListener(AppConfig appConfig) throws UnknownHostException, MongoException {
		this.appConfig = appConfig;
		
		m = MongoDBHandler.getNewMongoConnection();
		m.setWriteConcern(WriteConcern.SAFE);
		twitterDb = m.getDB(this.appConfig.getDBName());
		tweetCollection = twitterDb.getCollection(TwitterCollections.tweets.name());
		usersWaitingListColl = twitterDb.getCollection(TwitterCollections.usersWaitingList.name());
	}
	
	public void onStatus(Status status) {
		try {
			/*** Store the tweet into the database ***/
			String json = DataObjectFactory.getRawJSON(status);
			DBObject dbObject = (DBObject)JSON.parse(json);
			tweetCollection.insert(dbObject);
			
			// logger.debug("Received tweet from stream: " + status.getText());
			/*** Check if any user was mentioned. If yes, then check if his user id needs to saved in the users table ***/
			UserMentionEntity[] mentionedEntities = status.getUserMentionEntities();
			if (appConfig.isFollowMentions()) {
				if(mentionedEntities != null && mentionedEntities.length != 0) {
					addUsersToUsersWaitingListCollection(mentionedEntities, usersWaitingListColl);
				}
			}
			
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
    
    private void addUsersToUsersWaitingListCollection(UserMentionEntity[] mentionEntities, DBCollection usersWaitingListColl) {
		for (UserMentionEntity userMention : mentionEntities) {
			try {
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
