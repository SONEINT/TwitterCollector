package edu.isi.webserver;

import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.MongoException;

public class TestPlayground {

	public static void main(String[] args) throws UnknownHostException, MongoException {
		String regex = ".*?\\d+";
		Pattern p = Pattern.compile(regex,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		Matcher m = p.matcher("asdasd_2");
		System.out.println(m.group(0));
		
//		DateTime d = new DateTime(2012, 10, 22, 2, 30);
//		DateTime now = new DateTime();
//		
//		PeriodFormatter pf = new PeriodFormatterBuilder()
//		    .appendDays()
//		    .appendSuffix(" day", " days")
//		    .appendSeparator(" ")
//		    .appendHours()
//		    .appendSuffix(" hour", " hours")
//		    .appendSeparator(" ")
//		    .appendMinutes()
//		    .appendSuffix(" minute", " minutes")
//		    .appendSeparator(" and ")
//		    .appendSeconds()
//		    .appendSuffix(" second", " seconds")
//		    .toFormatter();
//		
//		Period p = new Period(d,now, PeriodType.dayTime());
//		System.out.println(d);
//		System.out.println(now);
//		System.out.println(p.toString(pf));
		
//		long[] users = {37032481};
//		FilterQuery fltrQry = new FilterQuery(users);
//		
//		ConfigurationBuilder cfg = TwitterApplicationManager.getOneConfigurationBuilderByTag(ApplicationTag.Search, "twitter");
//		
//		TwitterStream twitterStream = new TwitterStreamFactory(cfg.build()).getInstance();
//		twitterStream.addListener(new StatusListener() {
//
//			
//			
//			@Override
//			public void onException(Exception ex) {
//				// TODO Auto-generated method stub
//				
//			}
//
//			@Override
//			public void onStatus(Status status) {
//				// TODO Auto-generated method stub
//				System.out.println(status.getText());
//				System.out.println(status.getInReplyToStatusId());
//				System.out.println(status.getInReplyToUserId());
//				
//				UserMentionEntity[] mentionedEntities = status.getUserMentionEntities();
//				if(mentionedEntities != null && mentionedEntities.length != 0) {
//					System.out.println(mentionedEntities[0].getId());
//					System.out.println(mentionedEntities[0].getScreenName());
//				}
//				
//			}
//
//			@Override
//			public void onDeletionNotice(
//					StatusDeletionNotice statusDeletionNotice) {
//				// TODO Auto-generated method stub
//				
//			}
//
//			@Override
//			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
//				// TODO Auto-generated method stub
//				
//			}
//
//			@Override
//			public void onScrubGeo(long userId, long upToStatusId) {
//				// TODO Auto-generated method stub
//				
//			}
//			
//		});
//		// A new thread is automatically created by Twitter4j
//		twitterStream.filter(fltrQry);
		
//		System.out.println(new DateTime().toLocalDateTime());
//	
//		Mongo m = MongoDBHandler.getNewMongoConnection();
//		DB db = m.getDB("testdb");
		
		/** Create all the required collections **/
//		DBCollection statsColl = db.getCollection(TwitterCollections.linksStats.name());
//		DateTime t1 = new DateTime(2012, 10, 18, 10, 0);
//		insertIntoCollection(statsColl, t1.getMillis(), t1, 40000l);
//		DateTime t2 = new DateTime(2012, 10, 18, 10, 30);
//		insertIntoCollection(statsColl, t2.getMillis(), t2, 109873l);
//		DateTime t3 = new DateTime(2012, 10, 18, 11, 0);
//		insertIntoCollection(statsColl, t3.getMillis(), t3, 180000l);
//		DateTime t4 = new DateTime(2012, 10, 18, 11, 30);
//		insertIntoCollection(statsColl, t4.getMillis(), t4, 280000l);
//		DateTime t5 = new DateTime(2012, 10, 18, 12, 0);
//		insertIntoCollection(statsColl, t5.getMillis(), t5, 500000l);
//		DateTime t6 = new DateTime(2012, 10, 18, 12, 30);
//		insertIntoCollection(statsColl, t6.getMillis(), t6, 700000l);
		
		
//		DateTime t1 = new DateTime(2012, 10, 18, 10, 0);
//		insertIntoCollection(statsColl, t1.getMillis(), t1, 420000l);
//		DateTime t2 = new DateTime(2012, 10, 18, 10, 30);
//		insertIntoCollection(statsColl, t2.getMillis(), t2, 1094873l);
//		DateTime t3 = new DateTime(2012, 10, 18, 11, 0);
//		insertIntoCollection(statsColl, t3.getMillis(), t3, 1800400l);
//		DateTime t4 = new DateTime(2012, 10, 18, 11, 30);
//		insertIntoCollection(statsColl, t4.getMillis(), t4, 2280000l);
//		DateTime t5 = new DateTime(2012, 10, 18, 12, 0);
//		insertIntoCollection(statsColl, t5.getMillis(), t5, 7500000l);
//		DateTime t6 = new DateTime(2012, 10, 18, 12, 30);
//		insertIntoCollection(statsColl, t6.getMillis(), t6, 10700000l);
		
		System.out.println("done");
		
	}

//	private static void insertIntoCollection(DBCollection statsColl, long date, DateTime dateR, long count) {
//		BasicDBObject obj = new BasicDBObject(tweetsStats_SCHEMA.time.name(), date)
//			.append(tweetsStats_SCHEMA.timeReadable.name(), dateR.toString())
//			.append(tweetsStats_SCHEMA.count.name(), count); 
//		
//		statsColl.insert(obj);
//	}
}
