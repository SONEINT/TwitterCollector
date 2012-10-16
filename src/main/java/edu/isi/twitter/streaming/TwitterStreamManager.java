package edu.isi.twitter.streaming;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.MongoException;

import edu.isi.db.TwitterMongoDBHandler;
import edu.isi.twitter.AppConfig;
import edu.isi.twitter.TwitterApplicationManager;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;

public class TwitterStreamManager implements Runnable{
	
	private AppConfig appConfig;
	private static Logger logger = LoggerFactory.getLogger(TwitterStreamManager.class);
	private static int MAX_USERS_PER_APP = 5000;
	// private static int MAX_KEYWORDS_PER_APP = 400;
	private List<TwitterStream> currentStreams = new ArrayList<TwitterStream>();

	public TwitterStreamManager(AppConfig appConfig) {
		this.appConfig = appConfig;
	}

	private void deployAllStreams() {
		try {
			// Get all the Twitter configurations (for each app) for the streaming
			List<ConfigurationBuilder> allStreamingConfigs = TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.Streaming);
			int numberOfApps = allStreamingConfigs.size();
			
			// Get the list of users to filter stream
			long[] userIdsToFollow = TwitterMongoDBHandler.getCurrentFollowUserIdList(appConfig.getDBName(), numberOfApps * MAX_USERS_PER_APP);
			String[] keywords = TwitterMongoDBHandler.getSeedHashTagsList(appConfig.getDBName());
			
			logger.info("Number of users to filter on in Twitter stream: " + userIdsToFollow.length);
			logger.info("Number of keywords to track on in Twitter stream: " + keywords.length);
			
			// Start streaming filter
			int minNumberOfAppsRequired = (userIdsToFollow.length/MAX_USERS_PER_APP) + 1;
			
//			int minNumberOfAppsRequiredForUsers = (userIdsToFollow.length/MAX_USERS_PER_APP) + 1;
//			int minNumberOfAppsRequiredForKeywords = (keywords.length/MAX_KEYWORDS_PER_APP) + 1;
//			
//			int minNumberOfAppsRequired_1 = minNumberOfAppsRequiredForUsers > minNumberOfAppsRequiredForKeywords ? 
//					minNumberOfAppsRequiredForUsers : minNumberOfAppsRequiredForKeywords;
			
			if (minNumberOfAppsRequired > numberOfApps) {
				logger.info("More apps required to support streaming for all the users. Number of apps more required: " 
						+ (minNumberOfAppsRequired - numberOfApps));
				for (int i=0; i<numberOfApps; i++) {
					logger.info("Streaming App #: " + i);
					logger.info("Start index: " + (i * MAX_USERS_PER_APP));
					logger.info("End index: " + ((i * MAX_USERS_PER_APP) + (MAX_USERS_PER_APP)) );
					long[] users = Arrays.copyOfRange(userIdsToFollow, (i * MAX_USERS_PER_APP), ((i * MAX_USERS_PER_APP) + (MAX_USERS_PER_APP)));
					if (i == numberOfApps-1 && keywords.length != 0)
						startNewStreamingThread(users, keywords, allStreamingConfigs.get(i));
					else
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
						if (keywords.length != 0)
							startNewStreamingThread(users, keywords, allStreamingConfigs.get(i));
						else
							startNewStreamingThread(users, allStreamingConfigs.get(i));
					}
					else {
						logger.info("Start index: " + (i * numberOfUsersPerApp));
						logger.info("End index: " + ((i * numberOfUsersPerApp) + (numberOfUsersPerApp)) );
						users = Arrays.copyOfRange(userIdsToFollow, (i * numberOfUsersPerApp), ((i * numberOfUsersPerApp) + (numberOfUsersPerApp)));
						startNewStreamingThread(users, allStreamingConfigs.get(i));
					}
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
		twitterStream.addListener(new TwitterStreamListener(appConfig));
		// A new thread is automatically created by Twitter4j
		twitterStream.filter(fltrQry);
		currentStreams.add(twitterStream);
	}
	
	private void startNewStreamingThread(long[] users, String[] keywords, ConfigurationBuilder cb) throws UnknownHostException, MongoException {
		FilterQuery fltrQry = new FilterQuery();
		fltrQry.track(keywords).follow(users);
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStream.addListener(new TwitterStreamListener(appConfig));
		// A new thread is automatically created by Twitter4j
		twitterStream.filter(fltrQry);
		currentStreams.add(twitterStream);
	}

	@Override
	public void run() {
		while (true) {
			currentStreams.clear();
			deployAllStreams();
			try {
				Thread.sleep(TimeUnit.MINUTES.toMillis(15));
			} catch (InterruptedException e) {
				logger.error("Thread interrupted abruptly!", e);
			}
			// Close the streams properly before starting them again
			for (TwitterStream stream : currentStreams) {
				stream.shutdown();
			}
		}
	}
}
