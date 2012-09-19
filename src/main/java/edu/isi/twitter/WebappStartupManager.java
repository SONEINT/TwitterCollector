package edu.isi.twitter;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;
import edu.isi.twitter.rest.UserNetworkFetcherThread;
import edu.isi.twitter.rest.UserProfileFiller;
import edu.isi.twitter.rest.UserTweetsFetcherThread;
import edu.isi.twitter.streaming.TwitterUsersStreamDumper;

public class WebappStartupManager implements ServletContextListener {

	private Logger logger = LoggerFactory.getLogger(WebappStartupManager.class);
	
	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		WebappStartupManager mgr = new WebappStartupManager();
		mgr.clearOldThreadsFromTable();
		mgr.runTwitterStreamListenerThread();
		mgr.runUserProfileFillerThread();
		mgr.runUserNetworkFetcherThread();
		mgr.runTwitterTimeLineFetcher();
	}

	public static void main(String[] args) {
		WebappStartupManager mgr = new WebappStartupManager();
		mgr.clearOldThreadsFromTable();
		mgr.runTwitterStreamListenerThread();
		mgr.runUserProfileFillerThread();
		mgr.runUserNetworkFetcherThread();
		mgr.runTwitterTimeLineFetcher();
	}
	
	private void runTwitterStreamListenerThread() {
		logger.info("Starting Twitter stream listener thread...");
		Thread t = new Thread(new TwitterUsersStreamDumper(TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.Streaming)));
		t.start();
	}
	
	private void runUserProfileFillerThread() {
		logger.info("Starting User profile lokup thread...");
		Thread t = new Thread(new UserProfileFiller(TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.UserProfileLookup)));
		t.start();
	}
	
	private void runUserNetworkFetcherThread() {
		logger.info("Starting user network fetcher threads...");
		List<ConfigurationBuilder> allConfigs = TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.UserNetworkGraphFetcher);
		int minIteration = getMinimumIterationforTable(TwitterCollections.usersgraphlist);
		logger.info("Chosen min iteration # for network fetcher: " + minIteration);
		
		// Start a new thread for each application
		for(int i=0; i<allConfigs.size(); i++) {
			ConfigurationBuilder config = allConfigs.get(i);
			Thread t = new Thread(new UserNetworkFetcherThread(minIteration, config, i+1));
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
		List<ConfigurationBuilder> allConfigs = TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.UserTimelineFetcher);
		int minIteration = getMinimumIterationforTable(TwitterCollections.users);
		logger.info("Chosen min iteration # for timeline fetcher: " + minIteration);
		// Start a new thread for each application
		for(int i=0; i<allConfigs.size(); i++) {
			ConfigurationBuilder config = allConfigs.get(i);
			Thread t = new Thread(new UserTweetsFetcherThread(minIteration, config, i+1));
			t.start();
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private int getMinimumIterationforTable(TwitterCollections collection) {
		Mongo m = null;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
			DBCollection coll = twitterDb.getCollection(collection.name());

			DBCursor dbC = coll.find().sort(new BasicDBObject("iteration", 1)).limit(1);
			if (dbC.count() != 0)
				return Integer.parseInt(dbC.next().get("iteration").toString());
			else {
				logger.error("Something wrong in the query or table. No iteration found in " + collection.name());
				return 0;
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		} finally {
			m.close();
		}
		return 0;
	}
	
	private void clearOldThreadsFromTable() {
		logger.info("Removing all existing rows from the currentThreads table...");
		Mongo m;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			DB twitterDb = m.getDB(TwitterApplication.twitter.name());
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

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {}
}
