package edu.isi.twitter.streaming;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.UserMentionEntity;
import twitter4j.json.DataObjectFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;


public class TwitterStreamListener implements StatusListener {
	Mongo m;
	DB twitterDb;
	DBCollection tweetCollection;
	DBCollection userColl;
	DBCollection usersFromTweetMentionsColl;
	
	private static Logger logger = LoggerFactory.getLogger(TwitterStreamListener.class);
	
	public TwitterStreamListener() throws UnknownHostException, MongoException {
		m = MongoDBHandler.getNewMongoConnection();
		m.setWriteConcern(WriteConcern.SAFE);
		twitterDb = m.getDB(TwitterApplication.twitter.name());
		tweetCollection = twitterDb.getCollection(TwitterCollections.tweetsFromStream.name());
		userColl = twitterDb.getCollection(TwitterCollections.users.name());
		usersFromTweetMentionsColl = twitterDb.getCollection(TwitterCollections.usersFromTweetMentions.name());
	}
	
	public void onStatus(Status status) {
		try {
			/*** Store the tweet into the database ***/
			String json = DataObjectFactory.getRawJSON(status);
			DBObject dbObject = (DBObject)JSON.parse(json);
			tweetCollection.insert(dbObject);
			
			logger.debug("Received tweet from stream: " + status.getText());
			/*** Check if any user was mentioned. If yes, then check if his user id needs to saved in the users table ***/
			UserMentionEntity[] mentionedEntities = status.getUserMentionEntities();
			if(mentionedEntities != null && mentionedEntities.length != 0) {
				TwitterMongoDBHandler.addToUsersCollection(mentionedEntities, userColl, usersFromTweetMentionsColl);
			}
		} catch (MongoException e) {
			logger.error("Mongo Exception!", e);
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

}
