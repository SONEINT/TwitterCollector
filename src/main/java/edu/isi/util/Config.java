package edu.isi.util;

import edu.isi.util.Log;

import java.util.Iterator;
import java.util.HashMap;
import java.util.ResourceBundle;

public abstract class Config implements Iterable<String> {

	private HashMap<String, String> map = new HashMap<String, String>();

	public final void init(final String stem) {
		try {
			ResourceBundle bundle = ResourceBundle.getBundle(stem);
			for (String key : bundle.keySet()) {
				map.put(key, bundle.getString(key));
			}
			Log.getDefault().info("Loaded bundle from " + stem);
		} catch (Exception ex) {
			Log.getDefault().warn(
					"FAILED to load bundle from " + stem + ": "
							+ ex.getMessage());
		}
	}

	public Iterator<String> iterator() {
		return map.keySet().iterator();
	}

	public int getInt(final String key, final int defValue) {
		final String v = map.get(key);
		if (v != null) {
			try {
				return Integer.parseInt(v);
			} catch (NumberFormatException nfe) {
				System.err.println("TwitterConfig.getInt(" + key + ") --> " + v
						+ " is not a valid integer");
			}
		}
		return defValue;
	}

	public long getLong(final String key, final long defValue) {
		final String v = map.get(key);
		if (v != null) {
			try {
				return Long.parseLong(v);
			} catch (NumberFormatException nfe) {
				System.err.println("TwitterConfig.getInt(" + key + ") --> " + v
						+ " is not a valid integer");
			}
		}
		return defValue;
	}

	public String getString(final String key, final String defValue) {
		return get(key, defValue);
	}

	public String get(final String key) {
		return get(key, null);
	}

	public String get(final String key, final String defValue) {
		String v = map.get(key);
		return (v == null ? defValue : v);
	}

	/**
	 * Add to the config if not already there.
	 * 
	 * @return true if value was added
	 */
	public boolean add(final String key, final String value) {
		if (map.containsKey(key))
			return false;
		map.put(key, value);
		return true;
	}

	/**
	 * Overwrite a value in the config (add if not there)
	 * 
	 * @return old value (null if not there before)
	 */
	public String overwrite(final String key, final String value) {
		return map.put(key, value);
	}
}
