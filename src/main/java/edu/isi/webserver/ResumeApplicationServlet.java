package edu.isi.webserver;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;

import edu.isi.twitter.AppConfig;
import edu.isi.twitter.AppConfig.CONFIG_ATTRIBUTE;
import edu.isi.twitter.WebappStartupManager;
import edu.isi.webserver.WebAppManager.SERVLET_CONTEXT_ATTRIBUTE;

public class ResumeApplicationServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(ResumeApplicationServlet.class);
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		logger.info("Resuming application request received");
		AppConfig cfg = new AppConfig();
		
		String dbName = request.getParameter(CONFIG_ATTRIBUTE.DBName.name());
		cfg.setdBName(dbName);
		logger.info("Database name: " + dbName);
		int friendDepth = Integer.parseInt(request.getParameter(CONFIG_ATTRIBUTE.friendGraphDepth.name()));
		cfg.setFriendGraphDepth(friendDepth);
		int followerDepth = Integer.parseInt(request.getParameter(CONFIG_ATTRIBUTE.followerGraphDepth.name()));
		cfg.setFollowerGraphDepth(followerDepth);
		boolean followMentions = Boolean.parseBoolean(request.getParameter(CONFIG_ATTRIBUTE.followMentions.name()));
		cfg.setFollowMentions(followMentions);
		
		logger.info("Resuming application now ...");
		
		request.getServletContext().setAttribute(SERVLET_CONTEXT_ATTRIBUTE.appConfig.name(), cfg);
		logger.info("Config: " + cfg);
		
		// Create the StartupManager instance, store the stats object in ServletContext and start the application
		WebappStartupManager mgr = new WebappStartupManager(cfg);
		request.getServletContext().setAttribute(SERVLET_CONTEXT_ATTRIBUTE.statsMgr.name(), mgr.getStatisticsManager()); 
		try {
			mgr.resumeApplication();
		} catch (MongoException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		logger.info("Resuming application done!");
		response.sendRedirect("setup.html");
		response.flushBuffer();
	}
}
