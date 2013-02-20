package edu.isi.twitter.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import edu.isi.db.TwitterMongoDBHandler.USER_SOURCE;
import edu.isi.db.TwitterMongoDBHandler.usersWaitingList_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.users_SCHEMA;

public class UserNetworkFetcher {
	private long uid;
	private int friendDepth;
	private int followerDepth;
	private int followerCount;

	private Logger logger = LoggerFactory.getLogger(UserNetworkFetcher.class);
	
	public enum UserAction {
		init, add, delete
	}
	
	public enum LINK_TYPE {
		friend, follower
	}
	
	public UserNetworkFetcher(long uid, int friendDepth, int followerDepth) {
		this.uid = uid;
		this.friendDepth = friendDepth;
		this.followerDepth = followerDepth;
	}
	
	public int getFollowerCount() {
		return followerCount;
	}

	public boolean fetchAndStoreInDB(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, 
			Twitter authenticatedTwitter, DBCollection currentThreadsColl, DBObject threadObj, DBCollection usersColl) {
		boolean frSuccess = true;
		if (friendDepth > 0) {
			frSuccess = addLinks(usersGraphColl, usersGraphActionListColl, authenticatedTwitter, currentThreadsColl, 
					threadObj, usersColl, LINK_TYPE.friend);
		}
		boolean flwrSuccess = true;
		if (followerDepth > 0) {
			flwrSuccess = addLinks(usersGraphColl, usersGraphActionListColl, authenticatedTwitter, currentThreadsColl, 
					threadObj, usersColl, LINK_TYPE.follower);
		}
		return (frSuccess && flwrSuccess);
	}

	private boolean addLinks(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, Twitter authenticatedTwitter
			, DBCollection currentThreadsColl, DBObject threadObj, DBCollection usersColl, LINK_TYPE link_type) {
		// Create a flag that determines if the new users need to be added to the usersWaitingList collection
		boolean addToUsersWaitingList = false;
		if (link_type == LINK_TYPE.friend && friendDepth > 0)
			addToUsersWaitingList = true;
		else if (link_type == LINK_TYPE.follower && followerDepth > 0)
			addToUsersWaitingList = true;
		
		try {
			DateTime now = new DateTime();
			
			/** Getting all the links of the user from Twitter **/
			long cursor = -1;
			IDs ids = null;
			List<Long> linkTwitterList = new ArrayList<Long>();
			do {
				try {
					if (link_type == LINK_TYPE.follower)
						ids = authenticatedTwitter.getFollowersIDs(uid, cursor);
					else if (link_type == LINK_TYPE.friend)
						ids = authenticatedTwitter.getFriendsIDs(uid, cursor);
				} catch (TwitterException e) {
					// Taking care of the rate limiting
					if (e.exceededRateLimitation() || (e.getRateLimitStatus() != null && e.getRateLimitStatus().getRemaining() == 0)) {
						logger.info("Reached rate limit!");
						long timeToSleep = TimeUnit.MINUTES.toMillis(60); // Default sleep length = 60 minutes
						
						if (e.getRateLimitStatus().getSecondsUntilReset() > 0) {
							timeToSleep = TimeUnit.SECONDS.toMillis(e.getRateLimitStatus().getSecondsUntilReset() + 60);
						}
						try {
							logger.info("Making network fetcher thread sleep for " + TimeUnit.MILLISECONDS.toMinutes(timeToSleep) + " minutes.");
							
							threadObj.put("status", "sleeping");
							currentThreadsColl.save(threadObj);
							Thread.sleep(timeToSleep);
							threadObj.put("status", "notSleeping");
							currentThreadsColl.save(threadObj);
							
							logger.info("Waking up the network fetcher thread!");
							// Try again
							if (link_type == LINK_TYPE.follower)
								ids = authenticatedTwitter.getFollowersIDs(uid, cursor);
							else if (link_type == LINK_TYPE.friend)
								ids = authenticatedTwitter.getFriendsIDs(uid, cursor);
						} catch (InterruptedException e1) {
							logger.error("InterruptedException", e1);
						} catch (TwitterException e1) {
							logger.error("Error getting network information after waking up the thread.", e1);
							break;
						}
						
					} else
						logger.error("Problem occured while fetching network information: " + e.getMessage());
				}
				if (ids == null) {
					logger.error("Null ids found for the user: " + uid);
					break;
				}
					
				for (long id : ids.getIDs()) {
					linkTwitterList.add(new Long(id));
					
					// Increment the follower count
					if (link_type == LINK_TYPE.follower)
						followerCount++;
				}
			} while ((cursor = ids.getNextCursor()) != 0);
			
			/** Get all the existing links of the user from the Database and Update the last_seen flag for the common links **/
			DBCursor linkCursor = usersGraphColl.find(new BasicDBObject("uid", uid).append("link_type", link_type.name()));
			boolean linkDBListEmpty = linkCursor.count() == 0 ? true : false;
			List<Long> linkDBList = new ArrayList<Long>();
			while (linkCursor.hasNext()) {
				DBObject dbLink = linkCursor.next();
				long link_user_id = Long.parseLong(dbLink.get("link_user_id").toString());
				linkDBList.add(new Long(link_user_id));
				
				// Update the last seen if the user link already exists
				if (linkTwitterList.contains(new Long(link_user_id))) {
					dbLink.put("last_seen",now.toDate());
					usersGraphColl.save(dbLink);
				}
			}
			
			/** Add the new links in the usersGraph and usersGraphActionList **/
			List<Long> linkTwitterListCopy = new ArrayList<Long>(linkTwitterList);
			linkTwitterListCopy.removeAll(linkDBList);
			for (Long link_id : linkTwitterListCopy) {
				/** Add to the usersGraph **/
				DBObject link = new BasicDBObject("uid", uid).append("link_user_id", link_id)
						.append("link_type", link_type.name()).append("first_seen", now.toDate()).append("last_seen", now.toDate());
				usersGraphColl.save(link);
				
				/** Add to the usersGraphActionList **/
				DBObject action = new BasicDBObject("uid", uid).append("link_user_id", link_id)
						.append("link_type", link_type.name()).append("date", now.toDate());
				if(linkDBListEmpty)
					action.put("action", UserAction.init.name());
				else
					action.put("action", UserAction.add.name());
				usersGraphActionListColl.save(action);
				
				/** Add to the usersWaitingList if required **/
				if (addToUsersWaitingList) {
					addUserToUsersCollection(link_id, usersColl, link_type);
				}
			}
			
			/** Add "remove" action for any link of the user that is present in db list but not in the current Twitter link list **/
			linkDBList.removeAll(linkTwitterList);
			// Get the existing delete links that exist
			List<Long> existingDeleteLinksList = new ArrayList<Long>();
			DBObject deleteListingQuery = new BasicDBObject("uid", uid)
				.append("action", UserAction.delete.name()).append("link_type", link_type.name());
			DBCursor existingDeleteLinksC = usersGraphActionListColl.find(deleteListingQuery);
			while (existingDeleteLinksC.hasNext()) {
				DBObject dbLink = existingDeleteLinksC.next();
				long link_user_id = Long.parseLong(dbLink.get("link_user_id").toString());
				existingDeleteLinksList.add(new Long(link_user_id));
			}
				
			for (Long removedLinkId : linkDBList) {
				if(existingDeleteLinksList.contains(removedLinkId)) {
					continue;
				}
				else {
					// Add the delete link in userGraphAction table
					DBObject action = new BasicDBObject("uid", uid).append("link_user_id", removedLinkId)
							.append("action", UserAction.delete.name()).append("link_type", link_type.name()).append("date", now.toDate());
					usersGraphActionListColl.save(action);
				}
			}
			return true;
		} catch (Exception e) {
			logger.error("Error occured while getting user network", e);
			return false;
		}
	}

	private void addUserToUsersCollection(Long uid, DBCollection usersListColl, LINK_TYPE link_type) {
		DBObject usr = new BasicDBObject(usersWaitingList_SCHEMA.uid.name(), uid);
		usr.put(users_SCHEMA.source.name(), USER_SOURCE.Graph.name());
		usr.put(users_SCHEMA.friendDepth.name(), friendDepth-1);
		usr.put(users_SCHEMA.followerDepth.name(), followerDepth-1);
		usr.put(users_SCHEMA.followMentions.name(), false);
		usr.put(users_SCHEMA.passedGeospatialFilter.name(), true);
		usr.put(users_SCHEMA.nextUpdateTweetFetcherDate.name(), 0l);
		usr.put(users_SCHEMA.graphIterationCounter.name(), 0);
		
		
		try {
			usersListColl.insert(usr);
		} catch (MongoException e) {
			if (e.getCode() == 11000) {
				return;
			} else
				logger.error("Error adding user to users waiting list collection.", e);
		}
		
	}
}
