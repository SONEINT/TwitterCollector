package edu.isi.twitter;

import javax.ws.rs.core.MultivaluedMap;

import org.json.JSONException;
import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class GeonamesServiceInvoker {
	private Float lat;
	private Float lng;
	
	private static String COUNTRY_CODE_WEB_SERVICE_URL = "http://http://api.geonames.org/countryCode";
	
	public enum QUERY_PARAMS {
		lat, lng, username, type
	}
	
	public enum RESPONSE_PARAMS {
		countryCode, status, value
	}
	
	public GeonamesServiceInvoker(Float lat, Float lng) {
		this.lat = lat;
		this.lng = lng;
	}

	public String getCountryCode() {
		// Create the client
		Client client = Client.create();
		WebResource webResource = client.resource(COUNTRY_CODE_WEB_SERVICE_URL);
		
		// Prepare the parameters
		MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
		queryParams.add(QUERY_PARAMS.lat.name(), lat.toString());
		queryParams.add(QUERY_PARAMS.lng.name(), lng.toString());
		queryParams.add(QUERY_PARAMS.username.name(), "shuby.gupta");
		queryParams.add(QUERY_PARAMS.type.name(), "JSON");
		
		// Invoke the service
		String s = webResource.queryParams(queryParams).get(String.class);
		try {
			JSONObject responseObj = new JSONObject(s);
			
			// Return the country code if present
			if (responseObj.has(RESPONSE_PARAMS.countryCode.name())) {
				return responseObj.getString(RESPONSE_PARAMS.countryCode.name());
			}
			// Check if user account limit has been reached
			else if (responseObj.has(RESPONSE_PARAMS.status.name())) {
				JSONObject statusObj = responseObj.getJSONObject(RESPONSE_PARAMS.status.name());
				int statusVal = statusObj.getInt(RESPONSE_PARAMS.value.name());
				
				// If invalid lat/long kind of error
				if (statusVal == 12 || statusVal == 14 || statusVal == 15) {
					return "";
				} 
				// If hourly limit reached
				else if (statusVal == 19) {
					
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
			return "";
		}
		
		return "";
	}

}
