package edu.isi.twitter.streaming;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.MongoException;

import edu.isi.db.TwitterMongoDBHandler;
import edu.isi.twitter.TwitterApplicationManager;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;

public class TwitterStreamManager {
	
	private static Logger logger = LoggerFactory.getLogger(TwitterStreamManager.class);
	private static int MAX_USERS_PER_APP = 5000;

	public void deployAllThreads() {
		try {
			// Get the list of users to filter stream
			long[] userIdsToFollow = TwitterMongoDBHandler.getCurrentFollowUserIdList();
			logger.info("Number of users to filter on in Twitter stream: " + userIdsToFollow.length);
			
			// Get all the Twitter configurations (for each app) for the streaming
			List<ConfigurationBuilder> allStreamingConfigs = TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.Streaming);
			int numberOfApps = allStreamingConfigs.size();
			
			// Start streaming filter
			int minNumberOfAppsRequired = (userIdsToFollow.length/MAX_USERS_PER_APP) + 1;
			if (minNumberOfAppsRequired > numberOfApps) {
				logger.info("More apps required to support streaming for all the users. Number of apps more required: " 
						+ (minNumberOfAppsRequired - numberOfApps));
				for (int i=0; i<numberOfApps; i++) {
					logger.info("Streaming App #: " + i);
					logger.info("Start index: " + (i * MAX_USERS_PER_APP));
					logger.info("End index: " + ((i * MAX_USERS_PER_APP) + (MAX_USERS_PER_APP)) );
					long[] users = Arrays.copyOfRange(userIdsToFollow, (i * MAX_USERS_PER_APP), ((i * MAX_USERS_PER_APP) + (MAX_USERS_PER_APP)));
					startNewStreamingThread(users, allStreamingConfigs.get(i));
				}
			} else {
				int numberOfUsersPerApp = userIdsToFollow.length/numberOfApps + 1;
				logger.info("# users/app: " + numberOfUsersPerApp);
//				logger.info("Total users covered: " + numberOfUsersPerApp * numberOfApps);
				for (int i=0; i<numberOfApps; i++) {
					logger.info("Streaming App #: " + i);
					long[] users = null;
					if (i == numberOfApps-1) {	// Cover all the remaining users
						logger.info("Start index: " + (i * numberOfUsersPerApp));
						logger.info("End index: " + userIdsToFollow.length);
						users = Arrays.copyOfRange(userIdsToFollow, (i * numberOfUsersPerApp), userIdsToFollow.length);
					}
					else {
						logger.info("Start index: " + (i * numberOfUsersPerApp));
						logger.info("End index: " + ((i * numberOfUsersPerApp) + (numberOfUsersPerApp)) );
						users = Arrays.copyOfRange(userIdsToFollow, (i * numberOfUsersPerApp), ((i * numberOfUsersPerApp) + (numberOfUsersPerApp)));
					}
					startNewStreamingThread(users, allStreamingConfigs.get(i));
				}
			}
		} catch (MongoException e) {
			logger.error("Mongo exception. ", e);
		} catch (UnknownHostException e) {
			logger.error("Unknown host exception. ", e);
		}	
	}
	
	private void startNewStreamingThread(long[] users, ConfigurationBuilder cb) throws UnknownHostException, MongoException {
		FilterQuery fltrQry = new FilterQuery(users);
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStream.addListener(new TwitterStreamListener());
		// A new thread is automatically created by Twitter4j
		twitterStream.filter(fltrQry);
	}
}
