package edu.isi.twitter;

import edu.isi.util.Config;

public class TwitterConfig extends Config {
	public static final String TIMEOUT = "twitter.timeout";
	public static final String USER = "twitter.user";
	public static final String PSWD = "twitter.password";
	public static final String URL = "twitter.url";
	public static final String LIMIT = "twitter.per_file_limit";
	public static final String THREAD_JOIN = "twitter.thread_join";
	public static final String THREAD_DIE = "twitter.thread_seconds_to_die";
	public static final String ROOT = "twitter.root";
	public static final String DB = "twitter.db_resource_bundle";

	private TwitterConfig(final String stem) {
		add(TIMEOUT, "60000");
		add(LIMIT, "10000");
		add(THREAD_JOIN, "15");
		add(THREAD_DIE, "10");
		add(ROOT, ".");
		init(stem);
	}

	public static TwitterConfig getConfig() {
		return getConfig("twitter");
	}

	public static TwitterConfig getConfig(final String stem) {
		return new TwitterConfig(stem);
	}
}
