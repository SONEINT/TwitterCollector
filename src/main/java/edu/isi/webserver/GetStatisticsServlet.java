package edu.isi.webserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.linksStats_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.tweetsStats_SCHEMA;
import edu.isi.statistics.StatisticsManager;
import edu.isi.twitter.AppConfig;
import edu.isi.webserver.WebAppManager.SERVLET_CONTEXT_ATTRIBUTE;

public class GetStatisticsServlet extends HttpServlet {

	private static Logger logger = LoggerFactory.getLogger(GetStatisticsServlet.class);
	private static final long serialVersionUID = 1L;
	
	public enum OUTPUT_PARAM {
		tweetsStatus, tweetData, tweetStartTime, linksStatus, linksData, linksStartTime, statsMgrStatus, appStartTime,
		
		// User timeline API
		timelineUsersTraversedCount, timelineUsersPendingCount,
		
		// Users Graph API
		linksUsersTraversedCount, linksUsersPendingCount,
		
		// Search Query API for Hashtags
		searchHashtagsTraversedCount, searchHashtagsPendingCount,
		
		// Streaming API
		streamingUsersTraversedCount, streamingTweetsCount
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		AppConfig cfg = (AppConfig) request.getServletContext().getAttribute(SERVLET_CONTEXT_ATTRIBUTE.appConfig.name());
		if (cfg == null)
			return;
		Mongo m = MongoDBHandler.getNewMongoConnection();
		DB db = m.getDB(cfg.getDBName());
		
		try {
			/** Get the tweets statistics **/
			DBCollection statsColl = db.getCollection(TwitterCollections.tweetsStats.name());
			DBCursor cursor = statsColl.find();
			JSONObject outputObj = new JSONObject();
			String tweetsStatus = "success"; 
			if (cursor.count() == 0) {
				tweetsStatus = "error";
			} else {
				List<Long> times = new ArrayList<Long>();
				JSONArray countArray = new JSONArray();
				while (cursor.hasNext()) {
					DBObject row = cursor.next();
					times.add(Long.parseLong(row.get(tweetsStats_SCHEMA.time.name()).toString()));
					countArray.put(Long.parseLong(row.get(tweetsStats_SCHEMA.count.name()).toString()));
				}
				Long startTime = Collections.min(times);
				outputObj.put(OUTPUT_PARAM.tweetData.name(), countArray);
				outputObj.put(OUTPUT_PARAM.tweetStartTime.name(), startTime);
			}
			outputObj.put(OUTPUT_PARAM.tweetsStatus.name(), tweetsStatus);
			
			/** Get the link statistics **/
			DBCollection linksStatsColl = db.getCollection(TwitterCollections.linksStats.name());
			DBCursor linksCursor = linksStatsColl.find();
			
			String linksStatus = "success"; 
			if (linksCursor.count() == 0) {
				linksStatus = "error";
			} else {
				List<Long> times = new ArrayList<Long>();
				JSONArray countArray = new JSONArray();
				while (linksCursor.hasNext()) {
					DBObject row = linksCursor.next();
					times.add(Long.parseLong(row.get(linksStats_SCHEMA.time.name()).toString()));
					countArray.put(Long.parseLong(row.get(linksStats_SCHEMA.count.name()).toString()));
				}
				Long startTime = Collections.min(times);
				outputObj.put(OUTPUT_PARAM.linksData.name(), countArray);
				outputObj.put(OUTPUT_PARAM.linksStartTime.name(), startTime);
			}
			outputObj.put(OUTPUT_PARAM.linksStatus.name(), linksStatus);
			
			/** Get the API statistics **/
			StatisticsManager statsMgr = (StatisticsManager) request.getServletContext().getAttribute(SERVLET_CONTEXT_ATTRIBUTE.statsMgr.name());
			String statsMgrStatus = "success";
			if (statsMgr == null) {
				statsMgrStatus = "error";
			} else {
				long totalNumberOfCurrentUsers = db.getCollection(TwitterCollections.users.name()).count();
				long totalNumberOfHashtags = statsMgr.getTotalHashtagsCount();
				
				outputObj.put(OUTPUT_PARAM.appStartTime.name(), statsMgr.getAppUptimeDuration());
				
				/** Get the timeline API stats **/ 
				outputObj.put(OUTPUT_PARAM.timelineUsersTraversedCount.name(), statsMgr.getTimelineUsersTraversedCount());
				outputObj.put(OUTPUT_PARAM.timelineUsersPendingCount.name(), (totalNumberOfCurrentUsers - statsMgr.getTimelineUsersTraversedCount()));
				
				/** Get the friends/followers (links and user graph) API stats **/
				outputObj.put(OUTPUT_PARAM.linksUsersTraversedCount.name(), statsMgr.getLinksUsersTraversedCount());
				outputObj.put(OUTPUT_PARAM.linksUsersPendingCount.name(), (totalNumberOfCurrentUsers - statsMgr.getLinksUsersTraversedCount()));
				
				/** Get the streaming API stats **/
				outputObj.put(OUTPUT_PARAM.streamingUsersTraversedCount.name(), statsMgr.getStreamingUsersCount());
				outputObj.put(OUTPUT_PARAM.streamingTweetsCount.name(), statsMgr.getStreamingTweetsCount());
				
				/** Get the search query API stats **/
				outputObj.put(OUTPUT_PARAM.searchHashtagsTraversedCount.name(), statsMgr.getHashtagsTraversedCount());
				outputObj.put(OUTPUT_PARAM.searchHashtagsPendingCount.name(), (totalNumberOfHashtags - statsMgr.getHashtagsTraversedCount()));
			}
			outputObj.put(OUTPUT_PARAM.statsMgrStatus.name(), statsMgrStatus);
			
			/** Write the output JSON **/
			response.getWriter().write(outputObj.toString(2));
			response.flushBuffer();
		} catch (JSONException e) {
			logger.error("Unexpected JSON exception.", e);
		} finally {
			m.close();
		}
	}
}
