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
	
	public UserNetworkFetcher(long uid) {
		this.uid = uid;
	}

	public boolean fetchAndStoreInDB(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, Twitter authenticatedTwitter, DBCollection currentThreadsColl, DBObject threadObj) {
		boolean frSuccess = addFriends(usersGraphColl, usersGraphActionListColl, authenticatedTwitter, currentThreadsColl, threadObj);
		boolean flwrSuccess = addFollowers(usersGraphColl, usersGraphActionListColl, authenticatedTwitter, currentThreadsColl, threadObj);
		return (frSuccess && flwrSuccess);
	}

	private boolean addFriends(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, Twitter authenticatedTwitter, DBCollection currentThreadsColl, DBObject threadObj) {
		try {
			DateTime now = new DateTime();
			
			/** Getting all the friends of the user from Twitter **/
			long cursor = -1;
			IDs ids = null;
			List<Long> friendsTwitterList = new ArrayList<Long>();
			do {
				try {
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
								ids = authenticatedTwitter.getFriendsIDs(uid, cursor);
							}
						} catch (InterruptedException e1) {
							logger.error("InterruptedException", e1);
						}
					} else
						logger.error("Problem occured while fetching network information.", e);
				}
				if (ids == null) {
					logger.error("Null ids found for the user: " + uid);
					break;
				}
				for (long id : ids.getIDs()) {
					friendsTwitterList.add(new Long(id));
				}
			} while ((cursor = ids.getNextCursor()) != 0);
			
			/** Get all the existing friends of the user from the Database and Update the last_seen flag for the common friends **/
			DBCursor friendCursor = usersGraphColl.find(new BasicDBObject("uid", uid).append("link_type", "friend"));
			boolean friendDBListEmpty = friendCursor.count() == 0 ? true : false;
			List<Long> friendDBList = new ArrayList<Long>();
			while (friendCursor.hasNext()) {
				DBObject dbFriend = friendCursor.next();
				long link_user_id = Long.parseLong(dbFriend.get("link_user_id").toString());
				friendDBList.add(new Long(link_user_id));
				
				// Update the last seen if the user link already exists
				if (friendsTwitterList.contains(new Long(link_user_id))) {
					dbFriend.put("last_seen",now.toDate());
					usersGraphColl.save(dbFriend);
				}
			}
			
			/** Add the new friends in the usersGraph and usersGraphActionList **/
			List<Long> friendTwitterListCopy = new ArrayList<Long>(friendsTwitterList);
			friendTwitterListCopy.removeAll(friendDBList);
			for (Long link_id : friendTwitterListCopy) {
				/** Add to the usersGraph **/
				DBObject link = new BasicDBObject("uid", uid).append("link_user_id", link_id).append("link_type", "friend").append("first_seen", now.toDate()).append("last_seen", now.toDate());
				usersGraphColl.save(link);
				
				/** Add to the usersGraphActionList **/
				DBObject action = new BasicDBObject("uid", uid).append("link_user_id", link_id).append("link_type", "friend").append("date", now.toDate());
				if(friendDBListEmpty)
					action.put("action", UserAction.init.name());
				else
					action.put("action", UserAction.add.name());
				usersGraphActionListColl.save(action);
			}
			
			/** Add "remove" action for any friend of the user that is present in db list but not in the current Twitter friend list **/
			friendDBList.removeAll(friendsTwitterList);
			for (Long removedFriendId : friendDBList) {
				// Check if the delete link already exists
				DBObject query = new BasicDBObject("uid", uid).append("link_user_id", removedFriendId).append("action", UserAction.delete.name()).append("link_type", "friend");
				if(usersGraphActionListColl.find(query).count() != 0) {
					continue;
				}
				else {
					// Add the delete link in userGraphAction table
					query.put("date", now.toDate());
					usersGraphActionListColl.save(query);
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
	
	private boolean addFollowers(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, Twitter authenticatedTwitter, DBCollection currentThreadsColl, DBObject threadObj) {
		try {
			DateTime now = new DateTime();
			
			/** Getting all the followers of the user from Twitter **/
			long cursor = -1;
			IDs ids = null;
			List<Long> followerTwitterList = new ArrayList<Long>();
			do {
				try {
					ids = authenticatedTwitter.getFollowersIDs(uid, cursor);
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
								ids = authenticatedTwitter.getFollowersIDs(uid, cursor);
							} else {
								logger.info("Making network fetcher thread sleep for 60 minutes");
								
								threadObj.put("status", "sleeping");
								currentThreadsColl.save(threadObj);
								Thread.sleep(TimeUnit.MINUTES.toMillis(60));
								threadObj.put("status", "notSleeping");
								currentThreadsColl.save(threadObj);
								
								logger.info("Waking up the network fetcher thread!");
								// Try again
								ids = authenticatedTwitter.getFollowersIDs(uid, cursor);
							}
						} catch (InterruptedException e1) {
							logger.error("InterruptedException", e1);
						}
					} else
						logger.error("Problem occured while fetching network information.", e);
				}
				if (ids == null) {
					logger.error("Null ids found for the user: " + uid);
					break;
				}
					
				for (long id : ids.getIDs()) {
					followerTwitterList.add(new Long(id));
				}
			} while ((cursor = ids.getNextCursor()) != 0);
			
			/** Get all the existing followers of the user from the Database and Update the last_seen flag for the common followers **/
			DBCursor followerCursor = usersGraphColl.find(new BasicDBObject("uid", uid).append("link_type", "follower"));
			boolean followerDBListEmpty = followerCursor.count() == 0 ? true : false;
			List<Long> followerDBList = new ArrayList<Long>();
			while (followerCursor.hasNext()) {
				DBObject dbFollower = followerCursor.next();
				long link_user_id = Long.parseLong(dbFollower.get("link_user_id").toString());
				followerDBList.add(new Long(link_user_id));
				
				// Update the last seen if the user link already exists
				if (followerTwitterList.contains(new Long(link_user_id))) {
					dbFollower.put("last_seen",now.toDate());
					usersGraphColl.save(dbFollower);
				}
			}
			
			/** Add the new followers in the usersGraph and usersGraphActionList **/
			List<Long> followerTwitterListCopy = new ArrayList<Long>(followerTwitterList);
			followerTwitterListCopy.removeAll(followerDBList);
			for (Long link_id : followerTwitterListCopy) {
				/** Add to the usersGraph **/
				DBObject link = new BasicDBObject("uid", uid).append("link_user_id", link_id).append("link_type", "follower").append("first_seen", now.toDate()).append("last_seen", now.toDate());
				usersGraphColl.save(link);
				
				/** Add to the usersGraphActionList **/
				DBObject action = new BasicDBObject("uid", uid).append("link_user_id", link_id).append("link_type", "follower").append("date", now.toDate());
				if(followerDBListEmpty)
					action.put("action", UserAction.init.name());
				else
					action.put("action", UserAction.add.name());
				usersGraphActionListColl.save(action);
			}
			
			/** Add "remove" action for any follower of the user that is present in db list but not in the current Twitter follower list **/
			followerDBList.removeAll(followerTwitterList);
			for (Long removedFollowerId : followerDBList) {
				// Check if the delete link already exists
				DBObject query = new BasicDBObject("uid", uid).append("link_user_id", removedFollowerId).append("action", UserAction.delete.name()).append("link_type", "follower");
				if(usersGraphActionListColl.find(query).count() != 0) {
					continue;
				}
				else {
					// Add the delete link in userGraphAction table
					query.put("date", now.toDate());
					usersGraphActionListColl.save(query);
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
