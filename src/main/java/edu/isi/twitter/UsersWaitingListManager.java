package edu.isi.twitter;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.USER_SOURCE;
import edu.isi.db.TwitterMongoDBHandler.usersWaitingList_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.users_SCHEMA;
import edu.isi.filter.Filter;
import edu.isi.twitter.AppConfig.FILTER_TYPE;

public class UsersWaitingListManager implements Runnable {

	private AppConfig config;
	private static Logger logger = LoggerFactory.getLogger(UsersWaitingListManager.class);
	
	public UsersWaitingListManager(AppConfig config) {
		this.config = config;
	}
	
	@Override
	public void run() {
		try {
			Mongo m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
			DB twitterDb = m.getDB(config.getDBName());
			DBCollection usersColl = twitterDb.getCollection(TwitterCollections.users.name());
			DBCollection usersWaitingListColl = twitterDb.getCollection(TwitterCollections.usersWaitingList.name());
			DBObject query = new BasicDBObject(usersWaitingList_SCHEMA.profileCompleteAndChecked.name(), false);
			
			boolean isfilterOn = config.isFilterON();
			Filter filter = config.getFilter();
			
			while (true) {
				DBCursor cursor = usersWaitingListColl.find(query);
				
				/** For each user, check if the filter needs to be applied, check if the user needs to be added to the users collection **/
				while (cursor.hasNext()) {
					DBObject user = null;
					try {
						user = cursor.next();
						boolean userPassedFilter = true;
						USER_SOURCE source = USER_SOURCE.valueOf(user.get(usersWaitingList_SCHEMA.source.name()).toString());
						
						// If filter present, apply the filter to the user and add accordingly
						if (isfilterOn) {
							userPassedFilter = filter.filterUser(user);

							// If the filter type is hard, and the user did not pass the filter, skip the user.
							if (filter.getFilterType() == FILTER_TYPE.hard) {
								if(!userPassedFilter) {
									// Skip the user
									continue;
								}
								else {
									// Add to users collection
									addUserToUsersCollection(user, usersColl, userPassedFilter, true, 0, 0l, source);
								}
							} else if (filter.getFilterType() == FILTER_TYPE.soft) {
								// Still added to the user but with flag "passedGeospatialFilter" set according to the filter passed boolean value
								// Using a large value for graphIterationCounter for this user so that it is never selected to calculate graph 
								addUserToUsersCollection(user, usersColl, userPassedFilter, false, 20000, 1l, source);
							}
						}
						// If filter not present, add the user depending on the depths and source
						else {
							int friendDepth = Integer.parseInt(user.get(usersWaitingList_SCHEMA.friendDepth.name()).toString());
							int followerDepth = Integer.parseInt(user.get(usersWaitingList_SCHEMA.followerDepth.name()).toString());
							boolean followMentions = Boolean.parseBoolean(user.get(usersWaitingList_SCHEMA.followMentions.name()).toString());
							
							if (source == USER_SOURCE.Graph) {
								if (friendDepth >= 0 || followerDepth >= 0) {
									addUserToUsersCollection(user, usersColl, true, false, 0, 0l, source);
								}
							} else if (source == USER_SOURCE.Mentions) {
								if (followMentions) {
									addUserToUsersCollection(user, usersColl, true, false, 1, 1l, source);
								}
							} else {
								logger.error("Unknown source for user! Source should only come from enum values. Source: " + source);
							}
						}
						// Flag the user as its profile has been checked
						user.put(usersWaitingList_SCHEMA.profileCompleteAndChecked.name(), true);
						usersWaitingListColl.save(user);
					} catch (Exception e) {
						logger.error("Error occured while considering user: " + user + " for adding to the users collection.", e);
						continue;
					}
				}
			}
			
		}  catch (UnknownHostException e) {
			logger.error("UnknownHostException", e);
		} catch (MongoException e) {
			logger.error("Mongo Exception", e);
		}
	}

	private void addUserToUsersCollection(DBObject userObj, DBCollection usersColl, boolean userPassedFilter, boolean followMentionsVal, 
			int graphIterationCounterVal, long nextUpdateTweetFetcherDateVal, USER_SOURCE source) {
		try {
			userObj.put(users_SCHEMA.followMentions.name(), followMentionsVal);
			userObj.put(users_SCHEMA.passedGeospatialFilter.name(), userPassedFilter);
			userObj.put(users_SCHEMA.graphIterationCounter.name(), graphIterationCounterVal);
			userObj.put(users_SCHEMA.nextUpdateTweetFetcherDate.name(), nextUpdateTweetFetcherDateVal);
			userObj.put(users_SCHEMA.source.name(), source.name());
			usersColl.insert(userObj);
			
		} catch (MongoException e) {
			if (e.getCode() == 11000)	// User already exists
				return;
			else
				logger.error("Mongo Exception", e);
		}
		
	}
	
}
