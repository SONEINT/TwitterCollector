package edu.isi.filter;

import com.mongodb.DBObject;

import edu.isi.twitter.AppConfig.FILTER_TYPE;

public interface Filter {
	public boolean filterUser(DBObject user);
	public FILTER_TYPE getFilterType();
}
