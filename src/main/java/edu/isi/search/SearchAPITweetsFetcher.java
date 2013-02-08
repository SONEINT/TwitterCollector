package edu.isi.search;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.UserMentionEntity;
import twitter4j.json.DataObjectFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

import edu.isi.db.TwitterMongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TWEET_SOURCE;
import edu.isi.db.TwitterMongoDBHandler.tweets_SCHEMA;
import edu.isi.statistics.StatisticsManager;

public class SearchAPITweetsFetcher {
	
	private String queryString;
	private Twitter authenticatedTwitter;
	private QUERY_TYPE queryType;
	
	private static int RESULTS_PER_PAGE = 100;
	private static Logger logger = LoggerFactory.getLogger(SearchAPITweetsFetcher.class);
	
	public enum QUERY_TYPE {
		hashTag, userscreenname
	}
	
	public SearchAPITweetsFetcher(String queryString, Twitter authenticatedTwitter, QUERY_TYPE queryType) {
		this.queryString = queryString;
		this.authenticatedTwitter = authenticatedTwitter;
		this.queryType = queryType;
	}

	public boolean fetchAndStoreInDB(DBCollection tweetsCollection, DBCollection currentThreadsColl, 
			DBObject threadObj, DBCollection tweetsLogColl, DBCollection replyToColl, DBCollection mentionsColl, 
			DBCollection hashtagTweetsColl, StatisticsManager statsMgr) {
        Query query = new Query(getNormalizedQueryString());
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
					long timeToSleep = TimeUnit.MINUTES.toMillis(60); // Default sleep length = 60 minutes
					if (e.getRateLimitStatus().getSecondsUntilReset() > 0) {
						timeToSleep = TimeUnit.SECONDS.toMillis(e.getRateLimitStatus().getSecondsUntilReset() + 60);
					}
					try {
						logger.info("Reached rate limit! Making search API tweets fetcher thread sleep for " + TimeUnit.MILLISECONDS.toMinutes(timeToSleep) + " minutes.");
						
						threadObj.put("status", "sleeping");
						currentThreadsColl.save(threadObj);
						Thread.sleep(TimeUnit.SECONDS.toMillis(e.getRateLimitStatus().getSecondsUntilReset() + 60));
						threadObj.put("status", "notSleeping");
						currentThreadsColl.save(threadObj);

						logger.info("Waking up the search API tweets fetcher thread!");
						// Try again after waking up
						result = authenticatedTwitter.search(query);
					} catch (TwitterException e1) {
						logger.error("Error getting search API tweets after waking up the thread!", e1);
						break;
					} catch (InterruptedException e1) {
						logger.error("InterruptedException", e1);
					}
				} else {
					logger.error("Problem occured with search API: " + queryString + ". Error message: " + e.getMessage());
					return false;
				}
        	}
        	
        	if (result == null || result.getTweets().size() == 0)
        		break;
        	
            List<Tweet> tweets = result.getTweets();
//            if(tweets.size() == 1 && maxId != 0l)
//            	break;
            
            for (int i=0; i<tweets.size(); i++) {
            	Tweet tweet = tweets.get(i);
                String json = DataObjectFactory.getRawJSON(tweet);
				DBObject dbObject = (DBObject)JSON.parse(json);
				if(dbObject != null) {
					try {
						dbObject.put(tweets_SCHEMA.tweetCreatedAt.name(), tweet.getCreatedAt());
						dbObject.put(tweets_SCHEMA.APISource.name(), TWEET_SOURCE.Search.name());
						tweetsCollection.insert(dbObject);

						DateTime now = new DateTime();
						// Add this tweet's information in the tweetsLog collection
						TwitterMongoDBHandler.addToTweetLogTable(tweetsLogColl, TWEET_SOURCE.Search.name(), 
								tweet.getId(), now.getMillis(), tweet.getCreatedAt().getTime());
						
						// Add to replyTo table is the tweet was posted in reply to some previous tweet
						if (tweet.getInReplyToStatusId() != -1) {
							TwitterMongoDBHandler.addToReplyToTable(replyToColl, tweet.getId(), tweet.getInReplyToStatusId()
									, tweet.getFromUserId(), tweet.getToUserId(), now.getMillis(), tweet.getCreatedAt().getTime());
						}
						
						// Add to userWaitingList collection if required
						UserMentionEntity[] mentionedEntities = tweet.getUserMentionEntities();
						if (mentionedEntities != null && mentionedEntities.length != 0) {
							for (UserMentionEntity userMention : mentionedEntities) {
								TwitterMongoDBHandler.addToMentionsTable(mentionsColl, tweet.getId(), tweet.getFromUserId(), 
										userMention.getId(), now.getMillis(), tweet.getCreatedAt().getTime());
							}
						}
						
						// Add to hashtagtweets table if the sesrch entity is a hashtag
						if (queryType == QUERY_TYPE.hashTag) {
							TwitterMongoDBHandler.addTohashTagTweetsTable(hashtagTweetsColl, queryString, tweet.getId(), 
									tweet.getFromUserId(), now.getMillis(), tweet.getCreatedAt().getTime());
						}
						
						// Increment the tweet counter for it in the Statistics Manager
						statsMgr.incrementTweetCounterForHashtag(queryString);
					} catch (MongoException e) {
						// logger.error(e.getMessage());
						if(e.getCode() == 11000) {
							// Do nothing
//							logger.info("Tweet already exists!");
						}
						else {
							logger.error("Mongo Exception: " + e.getMessage());
						}
					}
				}
            	
				if(i == tweets.size()-1)
					maxId = tweet.getId()-1;
            }
            // Sleeping thread for 3 seconds to avoid generating requests too fast
            try {
//            	logger.info("sleeping");
				Thread.sleep(TimeUnit.SECONDS.toMillis(3));
//				logger.info("waking up");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
		return true;
	}

	private String getNormalizedQueryString() {
		String searchQuery = queryString;
		if (queryType == QUERY_TYPE.userscreenname)
        	searchQuery = "from:" + queryString + " OR to:"+queryString + " OR @"+queryString;
		return searchQuery;
	}
}
