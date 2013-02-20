package edu.isi.webserver;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.applications_SCHEMA;
import edu.isi.search.HashTagTweetsFetcherThread;
import edu.isi.statistics.StatisticsManager;
import edu.isi.twitter.AppConfig;
import edu.isi.twitter.AppConfig.CONFIG_ATTRIBUTE;
import edu.isi.twitter.TwitterApplicationManager;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;
import edu.isi.twitter.rest.UserNetworkFetcherThread;
import edu.isi.twitter.rest.UserProfileFillerThread;
import edu.isi.twitter.rest.UserTweetsFetcherThread;
import edu.isi.webserver.WebAppManager.SERVLET_CONTEXT_ATTRIBUTE;

public class AddApplicationServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(AddApplicationServlet.class);
	
	private enum STATUS_VALUE {
		Success, Error
	}
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String status = STATUS_VALUE.Success.name();
		String message = "Application added to the table.";
		
		String dbName = request.getParameter(CONFIG_ATTRIBUTE.DBName.name());
		String appName = request.getParameter(applications_SCHEMA.user_id.name());
		String consumerKey = request.getParameter(applications_SCHEMA.consumer_key.name());
		String consumerKeySecret = request.getParameter(applications_SCHEMA.consumer_key_secret.name());
		String accessToken = request.getParameter(applications_SCHEMA.access_token.name());
		String accessTokenSecret = request.getParameter(applications_SCHEMA.access_token_secret.name());
		String twitterAPI = request.getParameter(applications_SCHEMA.tag.name());
		
		try {
			Mongo m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
			DB twitterDb = m.getDB(dbName);
			DBCollection appColl = twitterDb.getCollection(TwitterCollections.applications.name());
			
			DBObject appObj = new BasicDBObject()
				.append(applications_SCHEMA.user_id.name(), appName)
				.append(applications_SCHEMA.consumer_key.name(), consumerKey)
				.append(applications_SCHEMA.consumer_key_secret.name(), consumerKeySecret)
				.append(applications_SCHEMA.access_token.name(), accessToken)
				.append(applications_SCHEMA.access_token_secret.name(), accessTokenSecret)
				.append(applications_SCHEMA.tag.name(), twitterAPI);
			
			appColl.insert(appObj);
		} catch (MongoException e) {
			status = STATUS_VALUE.Error.name();
			if (e.getCode() == 11000)
				message = "Application with same name already exists!";
			else
				message = e.getMessage();
		}
		
		JSONObject obj = new JSONObject();
		try {
			obj.put("status", status);
			obj.put("message", message);
			
			// Take action according to the Twitter API tag if the app is already running
			AppConfig cfg = (AppConfig) request.getServletContext().getAttribute(SERVLET_CONTEXT_ATTRIBUTE.appConfig.name());
			if (cfg != null && !status.equals(STATUS_VALUE.Error.name())) {
				StatisticsManager statsMgr = (StatisticsManager) request.getServletContext().getAttribute(SERVLET_CONTEXT_ATTRIBUTE.statsMgr.name());
				ConfigurationBuilder cb = TwitterApplicationManager.getOneConfigurationBuilderByAppName(appName, dbName);
				if (cb != null && statsMgr != null) {
					// User timeline
					if (twitterAPI.equals(ApplicationTag.UserTimelineFetcher.name())) {
						logger.info("Deploying User timeline fetcher thread for app: " + appName);
						Thread t = new Thread(new UserTweetsFetcherThread(cb, 99, cfg, statsMgr));
						t.start();
					}
					// Network fetcher
					else if (twitterAPI.equals(ApplicationTag.UserNetworkGraphFetcher.name())) {
						logger.info("Deploying User network graph fetcher thread for app: " + appName);
						Thread t = new Thread(new UserNetworkFetcherThread(cb, 99, cfg, statsMgr));
						t.start();
					}
					// Search API
					else if (twitterAPI.equals(ApplicationTag.Search.name())) {
						logger.info("Deploying Search API thread for app: " + appName);
						Thread t = new Thread(new HashTagTweetsFetcherThread(cb, 99, cfg, statsMgr));
						t.start();
					}
					// User profile lookup
					else if (twitterAPI.equals(ApplicationTag.UserProfileLookup.name())) {
						logger.info("Deploying UUser profile lookup thread for app: " + appName);
						Thread t = new Thread(new UserProfileFillerThread(cb, cfg));
						t.start();
					}
					// Streaming
					else if (twitterAPI.equals(ApplicationTag.Streaming.name())) {
						// Do nothing since the streaming manager should automatically pick up this app
					}
				} else {
					logger.error("Error occured while deploying thread:" + appName);
					obj.put("status", STATUS_VALUE.Error.name());
					obj.put("message", "Error occured while deploying thread for :" + appName);
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		response.setCharacterEncoding("UTF-8");
		response.getWriter().write(obj.toString());
		response.flushBuffer();
	}
}
