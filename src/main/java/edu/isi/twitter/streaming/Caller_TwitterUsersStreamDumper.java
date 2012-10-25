package edu.isi.twitter.streaming;

import twitter4j.conf.ConfigurationBuilder;

public class Caller_TwitterUsersStreamDumper implements Runnable {
	ConfigurationBuilder cb;
	
	// private static Logger logger = LoggerFactory.getLogger(TwitterUsersStreamDumper.class);
	
	public Caller_TwitterUsersStreamDumper (ConfigurationBuilder cb) {
		this.cb = cb;
	}

	public void run() {
//		try {
//			long[] userIdsToFollow = TwitterMongoDBHandler.getCurrentFollowUserIdList("twitter");
//			long[] userIdsToFollow = {750028153};
//			FilterQuery fltrQry = new FilterQuery(userIdsToFollow);
//			
//			TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
//			TwitterStreamListener listener = new TwitterStreamListener();
//			twitterStream.addListener(listener);
//			twitterStream.filter(fltrQry);
//		} catch (MongoException e) {
//			logger.error("Mongo Exception!", e);
//		} catch (UnknownHostException e) {
//			logger.error("Mongo Unknown host exception!", e);
//		}
	}

}
