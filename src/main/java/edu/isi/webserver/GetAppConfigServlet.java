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

import com.mongodb.DB;
import com.mongodb.Mongo;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.twitter.AppConfig;
import edu.isi.twitter.AppConfig.CONFIG_ATTRIBUTE;
import edu.isi.twitter.TwitterApplicationManager;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;
import edu.isi.webserver.WebAppManager.SERVLET_CONTEXT_ATTRIBUTE;

public class GetAppConfigServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(GetAppConfigServlet.class);
	
	public enum OUTPUT_PARAM {
		appRunning, seedUsersCount, hashTagsCount, timelineFetcherThreadsCount, graphThreadCount, 
		userProfileLookupThreadsCount, streamingThreadsCount, searchThreadsCount 
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		AppConfig cfg = (AppConfig) request.getServletContext().getAttribute(SERVLET_CONTEXT_ATTRIBUTE.appConfig.name());
		JSONObject outputObj = new JSONObject();
		if (cfg == null) {
			try {
				outputObj.put(OUTPUT_PARAM.appRunning.name(), false);
			} catch (JSONException e) {
				logger.error("JSON Exception", e);
			}
		} else {
			Mongo m = null;
			try {
				m = MongoDBHandler.getNewMongoConnection();
				DB db = m.getDB(cfg.getDBName());
				
				// Get the App configuration stats
				outputObj.put(OUTPUT_PARAM.appRunning.name(), true);
				outputObj.put(CONFIG_ATTRIBUTE.DBName.name(), cfg.getDBName());
				outputObj.put(CONFIG_ATTRIBUTE.friendGraphDepth.name(), cfg.getFriendGraphDepth());
				outputObj.put(CONFIG_ATTRIBUTE.followerGraphDepth.name(), cfg.getFollowerGraphDepth());
				outputObj.put(CONFIG_ATTRIBUTE.followMentions.name(), cfg.isFollowMentions());
				outputObj.put(OUTPUT_PARAM.seedUsersCount.name(), db.getCollection(TwitterCollections.seedUsers.name()).count());
				outputObj.put(OUTPUT_PARAM.hashTagsCount.name(), db.getCollection(TwitterCollections.seedHashTags.name()).count());
				
				// Get the threads information
				outputObj.put(OUTPUT_PARAM.timelineFetcherThreadsCount.name(), 
						TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.UserTimelineFetcher, cfg.getDBName()).size());
				outputObj.put(OUTPUT_PARAM.graphThreadCount.name(), 
						TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.UserNetworkGraphFetcher, cfg.getDBName()).size());
				outputObj.put(OUTPUT_PARAM.userProfileLookupThreadsCount.name(), 
						TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.UserProfileLookup, cfg.getDBName()).size());
				outputObj.put(OUTPUT_PARAM.streamingThreadsCount.name(), 
						TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.Streaming, cfg.getDBName()).size());
				outputObj.put(OUTPUT_PARAM.searchThreadsCount.name(), 
						TwitterApplicationManager.getAllConfigurationBuildersByTag(ApplicationTag.Search, cfg.getDBName()).size());
				
			} catch (JSONException e) {
				logger.error("JSON Exception", e);
			} finally {
				m.close();
			}
		}
		response.getWriter().write(outputObj.toString());
		response.flushBuffer();
	}
}
