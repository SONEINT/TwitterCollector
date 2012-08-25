package edu.isi.db;

import edu.isi.util.Config;

public class DBConfig extends Config {
	// Properties Strings
	public static final String DB_USERNAME = "db.username";
	public static final String DB_PASSWORD = "db.password";
	public static final String DB_DRIVER = "db.driver";
	public static final String DB_URL = "db.url";
	public static final String DB_QUEUE_TABLE = "db.queue_table";
	public static final String DB_USER_TABLE = "db.user_table";
	public static final String DB_HASHTAG_TABLE = "db.hashtag_table";
	public static final String DB_USERMAP_TABLE = "db.usermap_table";
	public static final String DB_LOCMAP_TABLE = "db.location_table";

	private DBConfig(final String stem) {
		add(DB_USERNAME, "root");
		add(DB_PASSWORD, "root");
		add(DB_DRIVER, "com.mysql.jdbc.Driver");
		add(DB_URL, "jdbc:mysql://localhost:3306/twitter");
		add(DB_QUEUE_TABLE, "queue");
		add(DB_USER_TABLE, "users");
		add(DB_HASHTAG_TABLE, "hashtags");
		add(DB_USERMAP_TABLE, "user_map");
		add(DB_LOCMAP_TABLE, "user_loc");
		init(stem);
	}

	public static DBConfig getConfig() {
		return getConfig("db");
	}

	public static DBConfig getConfig(final String stem) {
		return new DBConfig(stem);
	}
}
