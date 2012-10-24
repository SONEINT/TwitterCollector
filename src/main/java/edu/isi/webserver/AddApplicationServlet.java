package edu.isi.webserver;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

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
import edu.isi.twitter.AppConfig.CONFIG_ATTRIBUTE;
import edu.isi.webserver.WebAppManager.SERVLET_CONTEXT_ATTRIBUTE;

public class AddApplicationServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println("Add application request received!");
		
		System.out.println("CONFIG from servlet context: " + request.getServletContext().getAttribute(SERVLET_CONTEXT_ATTRIBUTE.appConfig.name()));
		String status = "Success";
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
			status = "Error";
			if (e.getCode() == 11000)
				message = "Application with same name already exists!";
			else
				message = e.getMessage();
		}
		
		JSONObject obj = new JSONObject();
		try {
			obj.put("status", status);
			obj.put("message", message);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		response.setCharacterEncoding("UTF-8");
		response.getWriter().write(obj.toString());
		response.flushBuffer();
	}
}
