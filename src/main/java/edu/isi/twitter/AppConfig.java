package edu.isi.twitter;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletContext;

import edu.isi.filter.Filter;
import edu.isi.filter.GeospatialLocationFilter;

public class AppConfig {

	private int friendGraphDepth;
	private int followerGraphDepth;
	private boolean followMentions;
	private String dBName;
	private Filter filter;
	
	// private Logger logger = LoggerFactory.getLogger(AppConfig.class);
	
	public enum CONFIG_ATTRIBUTE {
		friendGraphDepth, followerGraphDepth, followMentions, geospatialFilterOn, gazetteerFilePath , filterType, DBName
	}
	
	public enum FILTER_TYPE {
		hard, soft
	}
	
	public AppConfig(ServletContext ctx) throws IOException, IllegalArgumentException {
		try {
			setFriendGraphDepth(Integer.parseInt(ctx.getInitParameter(CONFIG_ATTRIBUTE.friendGraphDepth.name())));
			setFollowerGraphDepth(Integer.parseInt(ctx.getInitParameter(CONFIG_ATTRIBUTE.followerGraphDepth.name())));
			setFollowMentions(Boolean.parseBoolean(ctx.getInitParameter(CONFIG_ATTRIBUTE.followMentions.name())));
			setdBName(ctx.getInitParameter(CONFIG_ATTRIBUTE.DBName.name()));
		} catch (IllegalArgumentException e) {
			throw e;
		}
		
		boolean geospatialFilterOn = Boolean.parseBoolean(ctx.getInitParameter(CONFIG_ATTRIBUTE.geospatialFilterOn.name()));
		if (geospatialFilterOn) {
			turnGeospatialFilterON(ctx.getInitParameter(CONFIG_ATTRIBUTE.gazetteerFilePath.name()), 
					FILTER_TYPE.valueOf(ctx.getInitParameter(CONFIG_ATTRIBUTE.filterType.name())));
		}
	}
	
	public AppConfig() {
	}

	public int getFriendGraphDepth() {
		return friendGraphDepth;
	}


	public int getFollowerGraphDepth() {
		return followerGraphDepth;
	}


	public Filter getFilter() {
		return filter;
	}


	public String getDBName() {
		return dBName;
	}


	public void setFriendGraphDepth(int friendGraphDepth) {
		this.friendGraphDepth = friendGraphDepth;
	}


	public void setFollowerGraphDepth(int followerGraphDepth) {
		this.followerGraphDepth = followerGraphDepth;
	}


	public void setdBName(String dBName) {
		this.dBName = dBName;
	}
	
	public boolean isFollowMentions() {
		return followMentions;
	}

	public void setFollowMentions(boolean followMentions) {
		this.followMentions = followMentions;
	}

	public void turnGeospatialFilterON(String gazetteerFilePath, FILTER_TYPE filterType) throws IOException {
		File gFile = new File(gazetteerFilePath);
		filter = new GeospatialLocationFilter(gFile, filterType, dBName);
	}
	
	public boolean isFilterON() {
		return (filter != null);
	}

	@Override
	public String toString() {
		return "AppConfig [friendGraphDepth=" + friendGraphDepth
				+ ", followerGraphDepth=" + followerGraphDepth
				+ ", followMentions=" + followMentions + ", dBName=" + dBName + "]";
	}
	
	public static AppConfig getTestConfig() {
		AppConfig testCfg = new AppConfig();
		testCfg.setdBName("twitter");
		testCfg.setFriendGraphDepth(0);
		testCfg.setFollowerGraphDepth(0);
		testCfg.setFollowMentions(false);
		
		return testCfg;
	}
}
