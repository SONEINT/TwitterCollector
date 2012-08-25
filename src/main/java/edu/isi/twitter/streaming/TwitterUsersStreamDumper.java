package edu.isi.twitter.streaming;

import java.net.UnknownHostException;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.MongoException;

public class TwitterUsersStreamDumper {
	
	public static String ACCESS_TOKEN = "37032481-54MWFJ5Jism7TzrTeVXhx1z1BD6PnxvVMI3Xey0";
	public static String ACCESS_TOKEN_SECRET = "qZchnJl7DrXsUrCZ7TSVz2Ypf6ZyJHwkZkKWeNM";
	public static String CONSUMER_KEY = "mG6jCsiJY7ialuZuDpZRA";
	public static String CONSUMER_SECRET = "OF1T09DREJ0QYNauXFTkUQmqqElS88dc5rS0dxGbao";

	public static void main(String[] args) {
//		MongoDBHandler dbHandler = new MongoDBHandler();
		try {
//			long[] userIdsToFollow = dbHandler.getUserIdList();
			long[] userIdsToFollow = {750028153};
			FilterQuery fltrQry = new FilterQuery(userIdsToFollow);
			
			ConfigurationBuilder cb = new ConfigurationBuilder()
		    	.setDebugEnabled(true)
		    	.setOAuthConsumerKey(CONSUMER_KEY)
		    	.setOAuthConsumerSecret(CONSUMER_SECRET)
		    	.setOAuthAccessToken(ACCESS_TOKEN)
		    	.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
		    	.setJSONStoreEnabled(true);
			
			
			TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
			TwitterStreamListener listener = new TwitterStreamListener();
			twitterStream.addListener(listener);
			twitterStream.filter(fltrQry);
		    
			
		} catch (MongoException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}

}
