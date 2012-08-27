package edu.isi.twitter.streaming;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.MongoException;

import edu.isi.db.TwitterMongoDBHandler;

public class TwitterUsersStreamDumper implements Runnable {
	ConfigurationBuilder cb;
	
	private static Logger logger = LoggerFactory.getLogger(TwitterUsersStreamDumper.class);
	
	public TwitterUsersStreamDumper (ConfigurationBuilder cb) {
		this.cb = cb;
	}

	public void run() {
		try {
			long[] userIdsToFollow = TwitterMongoDBHandler.getCurrentFollowUserIdList();
//			long[] userIdsToFollow = {750028153};
			FilterQuery fltrQry = new FilterQuery(userIdsToFollow);
			
			TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
			TwitterStreamListener listener = new TwitterStreamListener();
			twitterStream.addListener(listener);
			twitterStream.filter(fltrQry);
		    
			
		} catch (MongoException e) {
			logger.error("Mongo Exception!", e);
		} catch (UnknownHostException e) {
			logger.error("Mongo Unknown host exception!", e);
		}
	}

}
