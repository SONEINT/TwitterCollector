package edu.isi.search;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class UserTweetsFetcher {
	private String hashTag;
	private Twitter authenticatedTwitter;
	
	private static int RESULTS_PER_PAGE = 100;
	private static Logger logger = LoggerFactory.getLogger(SearchAPITweetsFetcher.class);
	
	public UserTweetsFetcher (String hashTag, Twitter authenticatedTwitter) {
		this.hashTag = hashTag;
		this.authenticatedTwitter = authenticatedTwitter;
	}

	public boolean fetchAndStoreInDB(DBCollection tweetsCollection, DBCollection currentThreadsColl, DBObject threadObj) {
            Query query = new Query(hashTag);
            query.setRpp(RESULTS_PER_PAGE);
            long maxId = 0l;
            
            while (true) {
            	if(maxId != 0l)
            		query.setMaxId(maxId);
            	QueryResult result = null;
            	try {
            		result = authenticatedTwitter.search(query);
            	} catch (TwitterException e) {
            		// Taking care of the rate limiting
    				if (e.exceededRateLimitation() || (e.getRateLimitStatus() != null && e.getRateLimitStatus().getRemainingHits() == 0)) {
    					if (e.getRateLimitStatus().getSecondsUntilReset() > 0) {
    						try {
    							logger.info("Reached rate limit! Making hashtag tweets fetcher thread sleep for " + e.getRateLimitStatus().getSecondsUntilReset() + " seconds.");
    							
    							threadObj.put("status", "sleeping");
    							currentThreadsColl.save(threadObj);
    							Thread.sleep(TimeUnit.SECONDS.toMillis(e.getRateLimitStatus().getSecondsUntilReset() + 60));
    							threadObj.put("status", "notSleeping");
    							currentThreadsColl.save(threadObj);

    							logger.info("Waking up the hashtag tweets fetcher thread!");
    							// Try again after waking up
    							result = authenticatedTwitter.search(query);
    						} catch (TwitterException e1) {
    							logger.error("Error getting hashtag tweets after waking up the thread!", e1);
    							break;
    						} catch (InterruptedException e1) {
    							logger.error("InterruptedException", e1);
							}
    					} else {
    						logger.info("Making hashtag tweets fetcher thread sleep for 15 minutes");
    						try {
    							threadObj.put("status", "sleeping");
    							currentThreadsColl.save(threadObj);
    							Thread.sleep(TimeUnit.MINUTES.toMillis(15));
    							threadObj.put("status", "notSleeping");
    							currentThreadsColl.save(threadObj);
    							
    							logger.info("Waking up the hashtag tweets fetcher thread!");
    							// Try again
    							result = authenticatedTwitter.search(query);
    						} catch (TwitterException e1) {
    							logger.error("Error getting hashtags tweets after waking up the thread!", e1);
    							break;
    						} catch (InterruptedException e1) {
    							logger.error("InterruptedException", e1);
							}
    					}
    				} else
    					logger.error("Problem occured with hashTag: " + hashTag + ". Error message: " + e.getMessage());
            	}
            	
            	if (result == null || result.getTweets().size() == 0)
            		break;
            	
                List<Tweet> tweets = result.getTweets();
                if(tweets.size() == 1 && maxId != 0l)
                	break;
                
                for (int i=0; i<tweets.size(); i++) {
                	Tweet tweet = tweets.get(i);
                    String json = DataObjectFactory.getRawJSON(tweet);
    				DBObject dbObject = (DBObject)JSON.parse(json);
    				if(dbObject != null) {
    					try {
    						dbObject.put("tweetCreatedAt", tweet.getCreatedAt());
    						tweetsCollection.insert(dbObject);
//    						UserMentionEntity[] mentionEntities = tweet.getUserMentionEntities();
//
//    						if(mentionEntities != null)
//    							addToUsersCollection(mentionEntities, userColl, usersFromTweetMentionsColl);
    					} catch (MongoException e) {
    						/** Break out as all the tweets older than this should already exists. **/
    						if(e.getCode() == 11000) {
    							return true;
    						}
    						else
    							logger.error("Mongo Exception: " + e.getMessage());
    					}
    				}
                	
    				if(i == tweets.size()-1)
    					maxId = tweet.getId();
                }
                // Sleeping thread for 2 seconds to avoid generating requests too fast
                try {
					Thread.sleep(TimeUnit.SECONDS.toMillis(2));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            }
		return true;
	}
}
