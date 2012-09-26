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

public class UserNetworkFetcher {
	private long uid;

	private Logger logger = LoggerFactory.getLogger(UserNetworkFetcher.class);
	
	public enum UserAction {
		init, add, delete
	}
	
	public enum LINK_TYPE {
		friend, follower
	}
	
	public UserNetworkFetcher(long uid) {
		this.uid = uid;
	}

	public boolean fetchAndStoreInDB(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, Twitter authenticatedTwitter, DBCollection currentThreadsColl, DBObject threadObj) {
		boolean frSuccess = addLinks(usersGraphColl, usersGraphActionListColl, authenticatedTwitter, currentThreadsColl, threadObj, LINK_TYPE.friend);
		boolean flwrSuccess = addLinks(usersGraphColl, usersGraphActionListColl, authenticatedTwitter, currentThreadsColl, threadObj, LINK_TYPE.follower);
		return (frSuccess && flwrSuccess);
	}

	private boolean addLinks(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, Twitter authenticatedTwitter, DBCollection currentThreadsColl, DBObject threadObj, LINK_TYPE link_type) {
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
					if (e.exceededRateLimitation() || (e.getRateLimitStatus() != null && e.getRateLimitStatus().getRemainingHits() == 0)) {
						logger.error("Reached rate limit!");
						try {
							if (e.getRateLimitStatus().getSecondsUntilReset() > 0) {
								logger.info("Making network fetcher thread sleep for " + e.getRateLimitStatus().getSecondsUntilReset());
								
								threadObj.put("status", "sleeping");
								currentThreadsColl.save(threadObj);
								Thread.sleep(TimeUnit.SECONDS.toMillis(e.getRateLimitStatus().getSecondsUntilReset() + 60));
								threadObj.put("status", "notSleeping");
								currentThreadsColl.save(threadObj);
								
								logger.info("Waking up the network fetcher thread!");
								// Try again
								if (link_type == LINK_TYPE.follower)
									ids = authenticatedTwitter.getFollowersIDs(uid, cursor);
								else if (link_type == LINK_TYPE.friend)
									ids = authenticatedTwitter.getFriendsIDs(uid, cursor);
							} else {
								logger.info("Making network fetcher thread sleep for 60 minutes");
								
								threadObj.put("status", "sleeping");
								currentThreadsColl.save(threadObj);
								Thread.sleep(TimeUnit.MINUTES.toMillis(60));
								threadObj.put("status", "notSleeping");
								currentThreadsColl.save(threadObj);
								
								logger.info("Waking up the network fetcher thread!");
								// Try again
								if (link_type == LINK_TYPE.follower)
									ids = authenticatedTwitter.getFollowersIDs(uid, cursor);
								else if (link_type == LINK_TYPE.friend)
									ids = authenticatedTwitter.getFriendsIDs(uid, cursor);
							}
						} catch (InterruptedException e1) {
							logger.error("InterruptedException", e1);
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
				DBObject link = new BasicDBObject("uid", uid).append("link_user_id", link_id).append("link_type", link_type.name()).append("first_seen", now.toDate()).append("last_seen", now.toDate());
				usersGraphColl.save(link);
				
				/** Add to the usersGraphActionList **/
				DBObject action = new BasicDBObject("uid", uid).append("link_user_id", link_id).append("link_type", link_type.name()).append("date", now.toDate());
				if(linkDBListEmpty)
					action.put("action", UserAction.init.name());
				else
					action.put("action", UserAction.add.name());
				usersGraphActionListColl.save(action);
			}
			
			/** Add "remove" action for any link of the user that is present in db list but not in the current Twitter link list **/
			linkDBList.removeAll(linkTwitterList);
			// Get the existing delete links that exist
			List<Long> existingDeleteLinksList = new ArrayList<Long>();
			DBObject deleteListingQuery = new BasicDBObject("uid", uid).append("action", UserAction.delete.name()).append("link_type", link_type.name());
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
					DBObject action = new BasicDBObject("uid", uid).append("link_user_id", removedLinkId).append("action", UserAction.delete.name()).append("link_type", link_type.name()).append("date", now.toDate());
					usersGraphActionListColl.save(action);
				}
			}
			return true;
		} catch (TwitterException e) {
			logger.error("Critical Twitter exception!", e);
			return false;
		} catch (Exception e) {
			logger.error("Error occured while getting user network", e);
			return false;
		}
	}
}
