package edu.isi.twitter.streaming;

import java.net.UnknownHostException;

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
import com.mongodb.util.JSON;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler;


public class TwitterStreamListener implements StatusListener {
	Mongo m;
	DB twitterDb;
	DBCollection tweetCollection;
	
	public TwitterStreamListener() throws UnknownHostException, MongoException {
		m = MongoDBHandler.getNewMongoConnection();
		twitterDb = m.getDB(TwitterMongoDBHandler.DB_NAME);
		tweetCollection = twitterDb.getCollection("tweets");
	}
	
	public void onStatus(Status status) {
		/*** Store the tweet into the database ***/
		String json = DataObjectFactory.getRawJSON(status);
		DBObject dbObject = (DBObject)JSON.parse(json);
		tweetCollection.insert(dbObject);
		
		/*** Check if any user was mentioned. If yes, then check if his user id needs to saved in the users table ***/
		UserMentionEntity[] mentionedEntities = status.getUserMentionEntities();
		if(mentionedEntities != null && mentionedEntities.length != 0) {
			for (UserMentionEntity mentionedEntity : mentionedEntities) {
				System.out.println(mentionedEntity);
			}
		}
				
        System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
    }

    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }

    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
    }

    public void onScrubGeo(long userId, long upToStatusId) {
        System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }

    public void onException(Exception ex) {
        ex.printStackTrace();
    }

}
