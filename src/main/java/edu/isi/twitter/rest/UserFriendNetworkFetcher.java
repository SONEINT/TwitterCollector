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

public class UserFriendNetworkFetcher {
	private String screenName;

	private static Logger logger = LoggerFactory.getLogger(UserFriendNetworkFetcher.class);
	
	public enum UserAction {
		init, add, delete
	}
	
	public UserFriendNetworkFetcher(String userScreenName) {
		this.screenName = userScreenName;
	}

	public void fetchAndStoreInDB(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, Twitter authenticatedTwitter) {
		addFriends(usersGraphColl, usersGraphActionListColl, authenticatedTwitter);
		addFollowers(usersGraphColl, usersGraphActionListColl, authenticatedTwitter);
	}

	private void addFriends(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, Twitter authenticatedTwitter) {
		try {
			/** Get all the existing friends of the user from the Database **/
			DBCursor friendCursor = usersGraphColl.find(new BasicDBObject("name", screenName).append("link_type", "friend"));
			List<Long> friendDBList = new ArrayList<Long>();
			while (friendCursor.hasNext()) {
				DBObject friend = friendCursor.next();
				friendDBList.add(Long.parseLong(friend.get("link_user_id").toString()));
			}
			
			/** Getting all the friends of the user from Twitter **/
			long cursor = -1;
			IDs ids;
			List<Long> friendsTwitterList = new ArrayList<Long>();
			do {
				ids = authenticatedTwitter.getFriendsIDs(screenName, cursor);
				DateTime now = new DateTime();
				for (long id : ids.getIDs()) {
					friendsTwitterList.add(id);
					DBObject query = new BasicDBObject("name", screenName)
						.append("link_user_id", id)
						.append("link_type", "friend");
					// If link already exists
					if (usersGraphColl.find(query).count() != 0) {
						DBObject user = usersGraphColl.findOne(query); 
						user.put("last_seen",now.toDate());
						usersGraphColl.save(user);
					} else {
						query.put("first_seen", now.toDate());
						query.put("last_seen", now.toDate());
						usersGraphColl.save(query);
						
						// Initialize the link in userGraphAction table
						DBObject initAction = new BasicDBObject("name", screenName)
							.append("link_user_id", id)
							.append("link_type", "friend")
							.append("date", now.toDate());
						
						if(friendDBList.size() == 0)
							initAction.put("action", UserAction.init.name());
						else
							initAction.put("action", UserAction.add.name());
						usersGraphActionListColl.save(initAction);
					}
				}
			} while ((cursor = ids.getNextCursor()) != 0);
			
			/** Add remove action for any friend of the user that is present in db list but not in the current Twitter friend list **/
			friendDBList.removeAll(friendsTwitterList);
			for (Long removedFriendId : friendDBList) {
				// Check if the delete link already exists
				DBObject query = new BasicDBObject("name", screenName)
					.append("link_user_id", removedFriendId)
					.append("action", UserAction.delete.name())
					.append("link_type", "friend");
				if(usersGraphActionListColl.find(query).count() != 0) {
					continue;
				}
				else {
					// Add the delete link in userGraphAction table
					query.put("time_added", new DateTime().toDate());
					usersGraphActionListColl.save(query);
				}
			}
		} catch (TwitterException e) {
			if(e.exceededRateLimitation()) {
				try {
					Thread.sleep(TimeUnit.SECONDS.toMillis(e.getRetryAfter()));
					// Try again
					addFriends(usersGraphColl, usersGraphActionListColl, authenticatedTwitter);
				} catch (InterruptedException e1) {
					logger.error("InterruptedException", e1);
				}
			}
		}
	}
	
	private void addFollowers(DBCollection usersGraphColl, DBCollection usersGraphActionListColl, Twitter authenticatedTwitter) {
		try {
			/** Get all the existing followers of the user from the Database **/
			DBCursor followerCursor = usersGraphColl.find(new BasicDBObject("name", screenName).append("link_type", "follower"));
			List<Long> followerDBList = new ArrayList<Long>();
			while (followerCursor.hasNext()) {
				DBObject follower = followerCursor.next();
				followerDBList.add(Long.parseLong(follower.get("link_user_id").toString()));
			}
			
			/** Getting all the followers of the user **/
			long cursor = -1;
			IDs ids = null;
			List<Long> followerTwitterList = new ArrayList<Long>();
			do {
				ids = authenticatedTwitter.getFollowersIDs(screenName, cursor);
				DateTime now = new DateTime();
				for (long id : ids.getIDs()) {
					followerTwitterList.add(id);
					DBObject query = new BasicDBObject("name", screenName)
						.append("link_user_id", id)
						.append("link_type", "follower");
					// If link already exists
					if (usersGraphColl.find(query).count() != 0) {
						DBObject user = usersGraphColl.findOne(query); 
						user.put("last_seen",now.toDate());
						usersGraphColl.save(user);
					} else {
						query.put("first_seen", now.toDate());
						query.put("last_seen", now.toDate());
						usersGraphColl.save(query);
						
						// Initialize the link in userGraphAction table
						DBObject initAction = new BasicDBObject("name", screenName)
							.append("link_user_id", id)
							.append("date", now.toDate())
							.append("link_type", "follower");
						
						if(followerDBList.size() == 0)
							initAction.put("action", UserAction.init.name());
						else
							initAction.put("action", UserAction.add.name());
						usersGraphActionListColl.save(initAction);
					}
				}
			} while ((cursor = ids.getNextCursor()) != 0);
			
			/** Add remove action for any friend of the user that is present in db list but not in the current Twitter friend list **/
			followerDBList.removeAll(followerTwitterList);
			for (Long removedFollowerId : followerDBList) {
				// Check if the delete link already exists
				DBObject query = new BasicDBObject("name", screenName)
					.append("link_user_id", removedFollowerId)
					.append("action", UserAction.delete.name())
					.append("link_type", "follower");
				if(usersGraphActionListColl.find(query).count() != 0) {
					continue;
				}
				else {
					// Add the delete link in userGraphAction table
					query.put("time_added", new DateTime().toDate());
					usersGraphActionListColl.save(query);
				}
			}
		} catch (TwitterException e) {
			if(e.exceededRateLimitation()) {
				try {
					Thread.sleep(TimeUnit.SECONDS.toMillis(e.getRetryAfter()));
					// Try again
					addFollowers(usersGraphColl, usersGraphActionListColl, authenticatedTwitter);
				} catch (InterruptedException e1) {
					logger.error("InterruptedException", e1);
				}
			}
		}
	}
}
