package edu.isi.statistics;

import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

public class StatisticsManager {
	private Set<Long> timelineUsersCovered = Collections.synchronizedSet(new HashSet<Long>());
	private Set<Long> linksUsersCovered = Collections.synchronizedSet(new HashSet<Long>());
//	private Set<Long> searchUsersCovered = Collections.synchronizedSet(new HashSet<Long>());
	private Set<String> hashtagsCovered = Collections.synchronizedSet(new HashSet<String>());
	private Map<String, AtomicInteger> hashTagsTweetCounter = new Hashtable<String, AtomicInteger>();
	private AtomicInteger streamingTweetsCounter = new AtomicInteger();
	
	private int streamingUsersCount;
	private int totalHashtagsCount;
	private final DateTime appCreationTime = new DateTime();
	
	public int getStreamingUsersCount() {
		return streamingUsersCount;
	}

	public void setStreamingUsersCount(int streamingUsersCount) {
		this.streamingUsersCount = streamingUsersCount;
	}

	public int getTotalHashtagsCount() {
		return totalHashtagsCount;
	}

	public void setTotalHashtagsCount(int totalHashtagsCount) {
		this.totalHashtagsCount = totalHashtagsCount;
	}

	public void createTweetCounterForHashtag(String hashtag) {
		hashTagsTweetCounter.put(hashtag, new AtomicInteger());
	}
	
	public boolean tweetCounterExistsForHashtag (String hashtag) {
		return (hashTagsTweetCounter.get(hashtag) != null);
	}
	
	public void incrementTweetCounterForHashtag(String hashTag) {
		hashTagsTweetCounter.get(hashTag).incrementAndGet();
	}
	
	public void incrementStreamingTweetCounter() {
		streamingTweetsCounter.incrementAndGet();
	}
	
	public long getStreamingTweetsCount() {
		return streamingTweetsCounter.longValue();
	}
	
	public String getAppUptimeDuration() {
		DateTime now = new DateTime();
		PeriodFormatter pf = new PeriodFormatterBuilder()
		    .appendDays()
		    .appendSuffix(" day", " days")
		    .appendSeparator(" ")
		    .appendHours()
		    .appendSuffix(" hour", " hours")
		    .appendSeparator(" ")
		    .appendMinutes()
		    .appendSuffix(" minute", " minutes")
		    .appendSeparator(" and ")
		    .appendSeconds()
		    .appendSuffix(" second", " seconds")
		    .toFormatter();
	
		Period p = new Period(appCreationTime, now, PeriodType.dayTime());
		return p.toString(pf);
	}

	public int getTimelineUsersTraversedCount() {
		return timelineUsersCovered.size();
	}
	
	public int getLinksUsersTraversedCount() {
		return linksUsersCovered.size();
	}
	
//	public int getSearchUsersCoveredCount() {
//		return searchUsersCovered.size();
//	}
	
	public int getHashtagsTraversedCount() {
		return hashtagsCovered.size();
	}
	
	public void addTimelineUserTraversed(Long uid) {
		timelineUsersCovered.add(uid);
	}
	
	public void addGraphLiskUserTraversed(Long uid) {
		linksUsersCovered.add(uid);
	}
	
//	public void addSearchQUeryUserTraversed(Long uid) {
//		searchUsersCovered.add(uid);
//	}
	
	public void addHashTagTraversed(String hashtag) {
		hashtagsCovered.add(hashtag);
	}
}
