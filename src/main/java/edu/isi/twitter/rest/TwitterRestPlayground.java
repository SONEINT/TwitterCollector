package edu.isi.twitter.rest;

import java.util.concurrent.TimeUnit;

import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterRestPlayground {
	public static String ACCESS_TOKEN = "750028153-PWyGgHuXrqvKl7eOc0UkKd6aeYDvKMmgqm6g2J2s";
	public static String ACCESS_TOKEN_SECRET = "uEjZ8MS4jju35hdMiR5125rYrGcwMFrbwLtP3j8b7Bg";
	public static String CONSUMER_KEY = "DpSC9yFJUl9EQTBexmCxiw";
	public static String CONSUMER_SECRET = "RgVnqf9tb6PlIYLoswgeuOq695bMZGsgmDxeFndcTA";
	
	public static void main(String[] args) {

		System.out.println(TimeUnit.DAYS.toMillis(100));
		System.exit(0);
		
		ConfigurationBuilder cb = new ConfigurationBuilder()
			.setDebugEnabled(true)
			.setOAuthConsumerKey(CONSUMER_KEY)
	    	.setOAuthConsumerSecret(CONSUMER_SECRET)
	    	.setOAuthAccessToken(ACCESS_TOKEN)
	    	.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
			.setJSONStoreEnabled(true);

		Twitter authenticatedTwitter = new TwitterFactory(cb.build()).getInstance();
		
		Paging paging = new Paging(1, 100);
		
		try {
			ResponseList<Status> retweets = authenticatedTwitter.getFavorites(37032481, paging);
			for (int i=0; i<retweets.size(); i++) {
				Status rt = retweets.get(i);
				System.out.println(rt.getText());
			}
			
			
		} catch (TwitterException e) {
			e.printStackTrace();
		}
	}

}
