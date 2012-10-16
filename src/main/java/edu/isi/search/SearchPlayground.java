package edu.isi.search;

import java.util.List;
import java.util.concurrent.TimeUnit;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import edu.isi.twitter.TwitterApplicationManager;
import edu.isi.twitter.TwitterApplicationManager.ApplicationTag;

public class SearchPlayground {
	
	public static void main(String[] args) throws InterruptedException {
		ConfigurationBuilder cfg = TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.Search);
		Twitter twitter = new TwitterFactory(cfg.build()).getInstance();
        try {
//            Query query = new Query("from:BillGates OR to:ShubhamGupta OR @ShubhamGupta");
//        	Query query = new Query("from:BillGates OR from:ShubhamGupta OR to:BillGates");
        	Query query = new Query("@BillGates");
            query.setRpp(100);
            long maxId = 0l;
            
            while (true) {
            	if(maxId != 0l)
            		query.setMaxId(maxId);
            	QueryResult result = twitter.search(query);
                List<Tweet> tweets = result.getTweets();
                if(tweets.size() == 1 && maxId != 0l)
                	break;
                
                for (int i=0; i<tweets.size(); i++) {
                	Tweet tweet = tweets.get(i);
                	String json = DataObjectFactory.getRawJSON(tweet);
                    System.out.println("@" + tweet.getFromUser() + " - " + json);
                    
                    if(i == tweets.size()-1)
                    	maxId = tweet.getId();
                }
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                System.out.println("################################" + maxId);
            }
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
        }
	}
}
