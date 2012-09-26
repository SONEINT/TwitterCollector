package edu.isi.twitter;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import edu.isi.db.MongoDBHandler;
import edu.isi.db.TwitterMongoDBHandler.TwitterApplication;
import edu.isi.db.TwitterMongoDBHandler.TwitterCollections;
import edu.isi.db.TwitterMongoDBHandler.countryCodes_SCHEMA;
import edu.isi.db.TwitterMongoDBHandler.timezones_SCHEMA;

public class UserLocationIdentifier {
	private String location;
	private String timezone;
	
	private static Logger logger = LoggerFactory.getLogger(UserLocationIdentifier.class);
	private static List<String> middleEastTimezones = new ArrayList<String>();
	private static List<String> middleEastCountryCodes = new ArrayList<String>();
	
	// Regular expression pattern for finding lat and long inside the location string from user profiles
	String re1=".*?";	// Non-greedy match on filler
    String re2="([+-]?\\d*\\.\\d+)(?![-+0-9\\.])";	// Float 1
    String re3=".*?";	// Non-greedy match on filler
    String re4="([+-]?\\d*\\.\\d+)(?![-+0-9\\.])";	// Float 2
    private Pattern latLongRegex = Pattern.compile(re1+re2+re3+re4,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	
    
	public UserLocationIdentifier(String location, String timezone) {
		this.location = location;
		this.timezone = timezone;
	}
	
	public static void setup() {
		// Setup mongodb
		Mongo m = null;
		try {
			m = MongoDBHandler.getNewMongoConnection();
			m.setWriteConcern(WriteConcern.SAFE);
		} catch (UnknownHostException e) {
			logger.error("UnknownHostException", e);
		} catch (MongoException e) {
			logger.error("MongoException", e);
		}
		if(m == null) {
			logger.error("Error getting connection to MongoDB! Cannot proceed with this thread.");
			return;
		}
		DB twitterDb = m.getDB(TwitterApplication.twitter.name());
		
		/** Populate list of known middle east timezones **/
		DBCollection tz = twitterDb.getCollection(TwitterCollections.timezones.name());
		DBCursor tzC = tz.find();
		while (tzC.hasNext()) {
			middleEastTimezones.add(tzC.next().get(timezones_SCHEMA.name.name()).toString());
		}
		
		/** Populate list of known middle east country codes **/
		DBCollection cc = twitterDb.getCollection(TwitterCollections.countryCodes.name());
		DBCursor ccC = cc.find();
		while (ccC.hasNext()) {
			middleEastCountryCodes.add(ccC.next().get(countryCodes_SCHEMA.countryCode.name()).toString());
		}
		m.close();
	}
	
	public boolean isLocationInMiddleEast() {
		// Check if the timezone is from the known middle east timezones
		if (isKnownMiddleEastTimezone(timezone)) {
			return true;
		}
		
		// Check if the lat long coordinates are from middle east country by using the reverse geocoding of Geonames
		if (hasLatLongCoordinates(location)) {
			List<Float> coordinate = getLatLongFromLocation(location);
			if (coordinate != null && coordinate.size() == 2) {
				GeonamesServiceInvoker ws = new GeonamesServiceInvoker(coordinate.get(0), coordinate.get(1));
				String countryCode = ws.getCountryCode();
				if (middleEastCountryCodes.contains(countryCode))
					return true;
			}
		} 
		// Check if the location and timezone has test that can be matched to our Lucene index of gazatteer data
		else {
			
		}
		return false;
	}

	private boolean isKnownMiddleEastTimezone(String timezone) {
		return middleEastTimezones.contains(timezone);
	}

	public boolean hasLatLongCoordinates(String location) {
	    return latLongRegex.matcher(location).find();
	}
	
	private List<Float> getLatLongFromLocation(String location) {
		Matcher m = latLongRegex.matcher(location);
	    List<Float> coordinate = new ArrayList<Float>();
	    if (m.find()) {
	    	try {
		        float float1 = Float.parseFloat(m.group(1));
		        float float2 = Float.parseFloat(m.group(2));
		        coordinate.add(float1);
		        coordinate.add(float2);
		        System.out.print("("+float1+")"+"("+float2+")"+"\n");
	    	} catch (Exception e) {
	    		return null;
	    	}
	    }
	    return coordinate;
	}
	
	private void latLongTester() {
		String loc1 = "47.248612,-122.256781";
		String loc2 = "iPhone: 38.954477,-76.993023";
		String loc3 = "iPhone: 51.492901,-0.252436";
		String loc4 = "†T: 37.794653,-122.393264";
		
		String re1=".*?";	// Non-greedy match on filler
	    String re2="([+-]?\\d*\\.\\d+)(?![-+0-9\\.])";	// Float 1
	    String re3=".*?";	// Non-greedy match on filler
	    String re4="([+-]?\\d*\\.\\d+)(?![-+0-9\\.])";	// Float 2

	    String loc5 = "San Francisco";
		String loc6 = "";
		String loc7 = "";
		String loc8 = "iPhone: 39.926037,-104.933586";
		String loc9 = "†T: 37.794653,-122.393264";
		
		Pattern p = Pattern.compile(re1+re2+re3+re4,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	    Matcher m = p.matcher(loc8);
	    if (m.find())
	    {
	        String float1=m.group(1);
	        String float2=m.group(2);
	        System.out.print("("+float1.toString()+")"+"("+float2.toString()+")"+"\n");
	    } else
	    	System.out.println("Nothing found!");
	}
}
