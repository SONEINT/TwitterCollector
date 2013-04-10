package edu.isi.twitter;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.USER_SOURCE;
import edu.isi.db.TwitterMongoDBHandler.seedUsers_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.users_SCHEMA;
import edu.isi.search.HashTagTweetsFetcherThread;
import edu.isi.statistics.StatisticsDataCollectionThread;
import edu.isi.statistics.StatisticsManager;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;
import edu.isi.twitter.rest.UserNetworkFetcherThread;
import edu.isi.twitter.rest.UserProfileFillerThread;
import edu.isi.twitter.rest.UserTweetsFetcherThread;
import edu.isi.twitter.streaming.TwitterStreamManager;

public class WebappStartupManager {

	private AppConfig appConfig;
	private StatisticsManager statsMgr = new StatisticsManager();
	private static Logger logger = LoggerFactory.getLogger(WebappStartupManager.class);

	
	public WebappStartupManager(AppConfig appConfig) {
		this.appConfig = appConfig;
	}
	
	public StatisticsManager getStatisticsManager() {
		return statsMgr;
	}
	
	public void startApplication() {
		try {
			TwitterMongoDBHandler.createCollectionsAndIndexes(appConfig.getDBName());
			initializeUsersCollection();
			deployHashTagsTweetsFetcherThreads();
			clearOldThreadsFromTable();
//			
//			// Start the threads
			runTwitterStreamListenerThread();
			runUserProfileFillerThread();
			runUserNetworkFetcherThread();
			runTwitterTimeLineFetcher();
			
//			runFakeDataCollectionThread();
			
			runStatisticsCollectionThread();
			

			
		} catch (IOException e) {
			logger.error("Error setting up the config!", e);
		} catch (IllegalArgumentException e) {
			logger.error("Illegal argument specified in web.xml. Error setting up the config!", e);
		} catch (MongoException e) {
			logger.error("Mongo exception while setting up the application!", e);
		} catch (InterruptedException e) {
			logger.error("Thread interrupted abruptly while setting up the application!", e);
		}
		logger.info(appConfig.toString());
	}
	
	public void resumeApplication() throws UnknownHostException, MongoException, InterruptedException {
		clearOldThreadsFromTable();
		
		// Start the threads
		runTwitterStreamListenerThread();
		runUserProfileFillerThread();
		runUserNetworkFetcherThread();
		runTwitterTimeLineFetcher();
		deployHashTagsTweetsFetcherThreads();
		runStatisticsCollectionThread();
	}

//	private void runFakeDataCollectionThread() throws UnknownHostException, MongoException {
//		new Thread(new Runnable() {
//			
//			@Override
//			public void run() {
//				Mongo m = null;
//				try {
//					m = MongoDBHandler.getNewMongoConnection();
//					m.setWriteConcern(WriteConcern.SAFE);
//				} catch (UnknownHostException e) {
//					logger.error("UnknownHostException", e);
//				} catch (MongoException e) {
//					logger.error("MongoException", e);
//				}
//				if(m == null) {
//					logger.error("Error getting connection to MongoDB! Cannot proceed with this thread.");
//					return;
//				}
//				
//				DB twitterDb = m.getDB(appConfig.getDBName());
//				DBCollection tweets = twitterDb.getCollection(TwitterCollections.tweets.name());
//				DBCollection links = twitterDb.getCollection(TwitterCollections.usersGraph.name());
//				
//				while (true) {
//					tweets.insert(new BasicDBObject("test", "test"));
//					links.insert(new BasicDBObject("test", "test"));
//					try {
//						TimeUnit.SECONDS.sleep(1);
//					} catch (InterruptedException e) {
//						
//					}
//				}
//			}
//		}).start();
//		
//	}

	private void runStatisticsCollectionThread() {
		Thread t = new Thread (new StatisticsDataCollectionThread(appConfig));
		t.start();
	}

	public static void main(String[] args) {
		try {
			WebappStartupManager mgr = new WebappStartupManager(AppConfig.getTestConfig());
			mgr.initializeUsersCollection();
			//mgr.deployHashTagsTweetsFetcherThreads();
		} catch (UnknownHostException e) {
			logger.error("Error determining host!", e);
		} catch (MongoException e) {
			logger.error("Mongo Exception!", e);
		}
		
		
//		mgr.clearOldThreadsFromTable();
//		mgr.runTwitterStreamListenerThread();
//		mgr.runUserProfileFillerThread();
//		mgr.runUserNetworkFetcherThread();
//		mgr.runTwitterTimeLineFetcher();
	}
	
	public AppConfig getConfig() {
		return appConfig;
	}
	
	public void setConfig(AppConfig cfg) {
		this.appConfig = cfg;
	}
	
	private void initializeUsersCollection() throws UnknownHostException, MongoException {
		/** Seed users are copied to the users collection and various depths are also added in the user object **/
		Mongo m = MongoDBHandler.getNewMongoConnection();
		DB db = m.getDB(appConfig.getDBName());
		DBCollection seedUsersColl = db.getCollection(TwitterCollections.seedUsers.name());
		DBCollection usersColl = db.getCollection(TwitterCollections.users.name());
		
		DBCursor cursor = seedUsersColl.find().addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		if (cursor.count() == 0) 
			return;

		// ConfigurationBuilder and Twitter instances to be used in case when only user screen names are present
		ConfigurationBuilder cb =  TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.UserProfileLookup, appConfig.getDBName());
		Twitter twitter = new TwitterFactory(cb.build()).getInstance();
		int userAddedCounter = 0;
        try {
            while(cursor.hasNext()) {
            	DBObject seedUserObj = cursor.next();

            	// Check if the users object contains atleast name or uid
            	if (!seedUserObj.containsField(seedUsers_SCHEMA.name.name()) && !seedUserObj.containsField(seedUsers_SCHEMA.uid.name())) {
            		logger.info("Name or uid not found for the user: " + seedUserObj);
            		continue;
            	}
            	
            	// Add the user to the users collection
            	if(seedUserObj.containsField("uid")) {
            		saveToUsersCollection(seedUserObj, usersColl);
            		userAddedCounter++;
            	} else {
            		try {
            			logger.info("Using name service for: " + seedUserObj);
						long uid = getUsersUID(seedUserObj.get(seedUsers_SCHEMA.name.name()).toString(), twitter);
						seedUserObj.put(users_SCHEMA.uid.name(), uid);
						saveToUsersCollection(seedUserObj, usersColl);
						userAddedCounter++;
					} catch (TwitterException e) {
						logger.info("Error occured while getting uid for the user: " + seedUserObj + " Error message: " + e.getMessage());
						continue;
					}
            	}
            }
        } finally {
            cursor.close();
            m.close();
        }
        logger.info("Successfully added " + userAddedCounter + " users to the users list.");
	}

	private void saveToUsersCollection(DBObject userObj, DBCollection usersColl) {
		userObj.put(users_SCHEMA.source.name(), USER_SOURCE.Seed.name());
		userObj.put(users_SCHEMA.friendDepth.name(), appConfig.getFriendGraphDepth());
		userObj.put(users_SCHEMA.followerDepth.name(), appConfig.getFollowerGraphDepth());
		userObj.put(users_SCHEMA.followMentions.name(), appConfig.isFollowMentions());
		userObj.put(users_SCHEMA.passedGeospatialFilter.name(), true);
		userObj.put(users_SCHEMA.nextUpdateTweetFetcherDate.name(), 0l);
		userObj.put(users_SCHEMA.graphIterationCounter.name(), 0);
		usersColl.save(userObj);
	}

	private long getUsersUID(String screenName, Twitter twitter) throws TwitterException {
		User user = twitter.showUser(screenName);
		return user.getId();
	}

	private void runTwitterStreamListenerThread() {
		logger.info("Starting Twitter stream listener threads...");
		Thread strMgrThread = new Thread(new TwitterStreamManager(appConfig, statsMgr));
		strMgrThread.start();
	}
	
	private void runUserProfileFillerThread() {
		logger.info("Starting User profile lokup thread...");
		Thread t = new Thread(new UserProfileFillerThread(TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.UserProfileLookup
				, appConfig.getDBName()), appConfig));
		t.start();
	}
	
	private void runUserNetworkFetcherThread() {
		logger.info("Starting user network fetcher threads...");
		List<ConfigurationBuilder> allConfigs = TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.UserNetworkGraphFetcher
				, appConfig.getDBName());
		
		// Start a new thread for each application
		for(int i=0; i<allConfigs.size(); i++) {
			ConfigurationBuilder cfg = allConfigs.get(i);
			Thread t = new Thread(new UserNetworkFetcherThread(cfg, i+1, appConfig, statsMgr));
			t.start();
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void runTwitterTimeLineFetcher() {
		logger.info("Starting user tweet fetcher threads...");
		List<ConfigurationBuilder> allConfigs = TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.UserTimelineFetcher
				, appConfig.getDBName());
		
		// Start a new thread for each application
		for(int i=0; i<allConfigs.size(); i++) {
			ConfigurationBuilder cfg = allConfigs.get(i);
			Thread t = new Thread(new UserTweetsFetcherThread(cfg, i+1, appConfig, statsMgr));
			t.start();
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void deployHashTagsTweetsFetcherThreads() throws UnknownHostException, MongoException, InterruptedException {
		/** Quickly check if the threads need to be deployed by checking if we have any seed hash tags **/
		if (TwitterMongoDBHandler.getSeedHashTagsList(appConfig.getDBName()).length == 0)
			return;
		statsMgr.setTotalHashtagsCount(TwitterMongoDBHandler.getSeedHashTagsList(appConfig.getDBName()).length);
		logger.info("Starting hash tags tweet fetcher threads...");
		List<ConfigurationBuilder> allConfigs = TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.Search
				, appConfig.getDBName());
		
		// Start a new thread for each application
		for(int i=0; i<allConfigs.size(); i++) {
			ConfigurationBuilder cfg = allConfigs.get(i);
			Thread t = new Thread(new HashTagTweetsFetcherThread(cfg, i+1, appConfig, statsMgr));
			t.start();
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
		}
		
	}

	
	private void clearOldThreadsFromTable() {
		logger.info("Removing all existing rows from the currentThreads table...");
		Mongo m;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(appConfig.getDBName());
			DBCollection currentThreadsColl = twitterDb.getCollection(TwitterCollections.currentThreads.name());

			// Clear all the existing rows
			currentThreadsColl.remove(new BasicDBObject());
			m.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
}
