package edu.isi.webserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.seedHashTags_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.seedUsers_SCHEMA;
import edu.isi.twitter.AppConfig;
import edu.isi.twitter.AppConfig.CONFIG_ATTRIBUTE;
import edu.isi.twitter.WebappStartupManager;
import edu.isi.webserver.WebAppManager.REQUEST_PARAMETER;
import edu.isi.webserver.WebAppManager.SERVLET_CONTEXT_ATTRIBUTE;


public class SetupServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static String DESTINATION_DIR_PATH = "UserUploadedFiles/";
	private static Logger logger = LoggerFactory.getLogger(SetupServlet.class);

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println("Setup Request received!");
		AppConfig cfg = new AppConfig();
		
		// Download the file to the upload file folder
		File destinationDir = new File(DESTINATION_DIR_PATH);
		logger.info("File upload destination directory: " + destinationDir.getAbsolutePath());
		if(!destinationDir.isDirectory()) {
			destinationDir.mkdir();
		}
		
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();

		// Set the size threshold, above which content will be stored on disk.
		fileItemFactory.setSizeThreshold(1*1024*1024); //1 MB

		//Set the temporary directory to store the uploaded files of size above threshold.
		fileItemFactory.setRepository(destinationDir);
 
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
		
		File uploadedFile = null;
		try {
			// Parse the request
			@SuppressWarnings("rawtypes")
			List items = uploadHandler.parseRequest(request);
			@SuppressWarnings("rawtypes")
			Iterator itr = items.iterator();
			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();

				// Ignore Form Fields.
				if(item.isFormField()) {
					
					if(item.getFieldName().equals(CONFIG_ATTRIBUTE.DBName.name()))
						cfg.setdBName(item.getString());
					else if(item.getFieldName().equals(CONFIG_ATTRIBUTE.friendGraphDepth.name()))
						cfg.setFriendGraphDepth(Integer.parseInt(item.getString()));
					else if(item.getFieldName().equals(CONFIG_ATTRIBUTE.followerGraphDepth.name()))
						cfg.setFollowerGraphDepth(Integer.parseInt(item.getString()));
					else if(item.getFieldName().equals(CONFIG_ATTRIBUTE.followMentions.name()))
						cfg.setFollowMentions(Boolean.parseBoolean(item.getString()));
					else if(item.getFieldName().equals(REQUEST_PARAMETER.seedUsersList.name()))
						populateSeedUsersCollectionFromList(item.getString(), cfg);
					else if(item.getFieldName().equals(REQUEST_PARAMETER.seedHashTagsList.name()))
						populateSeedHashTagsCollectionFromList(item.getString(), cfg);
					
				} else {
					//Handle Uploaded files. Write file to the ultimate location.
					System.out.println("File field name: " + item.getFieldName());
					if(item.getName() == null || item.getName().trim().equals(""))
						continue;
					
					uploadedFile = new File(destinationDir,item.getName());
					item.write(uploadedFile);
					System.out.println("File written to: " + uploadedFile.getAbsolutePath());
					
					if (item.getFieldName().equals(REQUEST_PARAMETER.seedHashTagsFile.name())) {
						String tagsList = FileUtils.readFileToString(uploadedFile);
						populateSeedHashTagsCollectionFromList(tagsList, cfg);
					}
					else if (item.getFieldName().equals(REQUEST_PARAMETER.seedUsersFile.name())) {
						String usersList = FileUtils.readFileToString(uploadedFile);
						populateSeedUsersCollectionFromList(usersList, cfg);
					}
				}
			}
		} catch(FileUploadException ex) {
			logger.error("Error encountered while parsing the request",ex);
		} catch(Exception ex) {
			logger.error("Error encountered while uploading file",ex);
		}
		
		
		request.getServletContext().setAttribute(SERVLET_CONTEXT_ATTRIBUTE.appConfig.name(), cfg);
		System.out.println("Config: " + cfg);
		
		// Create the StartupManager instance, store the stats object in ServletContext and start the application
		WebappStartupManager mgr = new WebappStartupManager(cfg);
		request.getServletContext().setAttribute(SERVLET_CONTEXT_ATTRIBUTE.statsMgr.name(), mgr.getStatisticsManager()); 
		mgr.startApplication();
		
//		response.setCharacterEncoding("UTF-8");
//		response.setContentType("text/html");
//		response.getWriter().write("App Deployed!");
		response.sendRedirect("setup.html");
		response.flushBuffer();
	}

	private void populateSeedUsersCollectionFromList(String usersList, AppConfig config) throws FileNotFoundException, UnknownHostException{
		Scanner scanner = new Scanner(usersList);
		
		Mongo m = MongoDBHandler.getNewMongoConnection();
		m.setWriteConcern(WriteConcern.SAFE);
		DB db = m.getDB(config.getDBName());
		DBCollection seedUsersColl = db.getCollection(TwitterCollections.seedUsers.name());
		seedUsersColl.ensureIndex(new BasicDBObject(seedUsers_SCHEMA.name.name(), 1), new BasicDBObject("unique", true).append("dropDups", true).append("sparse", true));
		seedUsersColl.ensureIndex(new BasicDBObject(seedUsers_SCHEMA.uid.name(), 1), new BasicDBObject("unique", true).append("dropDups", true).append("sparse", true));
		
		try {
			while (scanner.hasNext()) {
				// If its a UID
				if (scanner.hasNextLong()) {
					Long uid = scanner.nextLong();
					try {
						seedUsersColl.insert(new BasicDBObject(seedUsers_SCHEMA.uid.name(), uid));
					} catch (MongoException e) {
						if (e.getCode() == 11000) {
							logger.info("Duplicate user being inserted to the seed users list!");
						} else
							logger.error("Mongo exception while inserting user into seed user list!", e);
						continue;
					}
				}
				// Default is a screen name
				else {
					String screenName = scanner.next();
					if (screenName.trim().equals(""))
						continue;
					try {
						seedUsersColl.insert(new BasicDBObject(seedUsers_SCHEMA.name.name(), screenName));
					} catch (MongoException e) {
						if (e.getCode() == 11000) {
							logger.info("Duplicate user being inserted to the seed users list!");
							continue;
						}
						else
							logger.error("Mongo exception while inserting user into seed user list!", e);
						continue;
					}
				}
			}
		} finally {
			scanner.close();
			m.close();
		}
		
	}

	private void populateSeedHashTagsCollectionFromList(String tagsList, AppConfig config) throws FileNotFoundException, UnknownHostException {
		Scanner scanner = new Scanner(tagsList);
		
		Mongo m = MongoDBHandler.getNewMongoConnection();
		m.setWriteConcern(WriteConcern.SAFE);
		DB db = m.getDB(config.getDBName());
		DBCollection seedHashTagssColl = db.getCollection(TwitterCollections.seedHashTags.name());
		seedHashTagssColl.ensureIndex(new BasicDBObject(seedHashTags_SCHEMA.value.name(), 1), new BasicDBObject("unique", true).append("dropDups", true));
		
		
		try {
			while (scanner.hasNext()) {
				String seedTag = scanner.next();
				if (seedTag.trim().equals(""))
					continue;
				try {
					if (!seedTag.startsWith("#"))
						seedTag = "#" + seedTag;
					seedHashTagssColl.insert(new BasicDBObject(seedHashTags_SCHEMA.value.name(), seedTag));
				} catch (MongoException e) {
					if (e.getCode() == 11000) {
						logger.info("Duplicate hash tag being inserted to the seed hash tags list!");
					}
					else
						logger.error("Mongo exception while inserting hash tag into seed hash tags list!", e);
					continue;
				}
				
			}
		} finally {
			scanner.close();
			m.close();
		}
	}
}
