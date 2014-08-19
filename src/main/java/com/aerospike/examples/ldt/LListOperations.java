package com.aerospike.examples.ldt;

//import java.io.Console;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * LListOperations holds functions that manage the URL Site Visit data
 * with Aerospike Large List.
 * 
@author toby
*/
public class LListOperations implements ILdtOperations {
	private AerospikeClient client;
	private String namespace;
	private String set;
	private WritePolicy writePolicy;
	private Policy policy;

	protected Console console;

	/**
	 * Constructor for LLIST OPERATION class.
	 * @param client
	 * @param namespace
	 * @param set
	 * @param console
	 * @throws AerospikeException
	 */
	public LListOperations(AerospikeClient client, String namespace, String set, 
			Console console) throws AerospikeException 
	{

		this.client = client;
		this.namespace = namespace;
		this.set = set; // Set will be overridden by the data.
		this.writePolicy = new WritePolicy();
		this.writePolicy.timeout = 1000;
		this.writePolicy.maxRetries = 0;
		this.policy = new Policy();
		this.console = console;
	}
	
	public void setup() {
		// Nothing needed for LLIST (yet).
		// Eventually, we will register needed UDFs here.
	}


	/**
	 * Enter a new Site Visit object in the collection of site visits for
	 * a particular user.  Order the Site Visit Objects by Expire Time.
	 * @param commandObj
	 * @param params
	 */
	public void processNewSiteVisit( JSONObject commandObj  ) {
		console.info("ENTER ProcessNewSiteVisit:");

		JSONObject siteObj = (JSONObject) commandObj.get("visit_info");

		// Extract the values from the JSON object
		String nameStr = (String) siteObj.get("user_name");
		String urlStr = (String) siteObj.get("url");
		String refStr = (String) siteObj.get("referrer");
		String pageStr = (String) siteObj.get("page_title");
		Long dateInt = (Long) siteObj.get("date");
		Long expireInt = (Long) siteObj.get("expire");
		String customerStr = (String) siteObj.get("set_name");
		
		// The Customer ID (custID) is the Aerospike SET name, and userID is the
		// key for the record (the user data and the site visit list).
		String userID = nameStr;
		String custID = customerStr;

		try {

			Key userKey = new Key(this.namespace, custID, userID);
			String siteListBin = "Site List";

			// Create a MAP object that will hold the Site Visit value.
			// For THIS example, we're going to put the Site Visit object into
			// a Large List (LLIST), so we're going to use as an Object Key the
			// Expire Date (an integer).  That way, the Site Visit objects will
			// be kept in EXPIRE ORDER, and thus will be easy to scan and manage.
			HashMap<String,Object> siteObjMap = new HashMap<String,Object>();
			siteObjMap.put("key", expireInt);
			siteObjMap.put("name", nameStr);
			siteObjMap.put("URL", urlStr);
			siteObjMap.put("referrer", refStr);
			siteObjMap.put("page_title", pageStr);
			siteObjMap.put("date", dateInt);

			// Initialize large set operator.
			com.aerospike.client.large.LargeList llist = client.getLargeList(this.policy, userKey, siteListBin, null);

			// Package up the Map Object and add it to the LLIST.  Note that the
			// "Value.get()" operation is NOT used.  Instead it's Value.getAsMap().
			llist.add(Value.getAsMap(siteObjMap));			

		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e);
		}
	} // end processNewSiteVisit()


	/**
	 * Scan the user's Site Visit List.  Get back a List of Maps that we
	 * can peruse and print.
	 * @param commandObj
	 * @param params
	 */
	public void processSiteQuery( JSONObject commandObj  ) {
		System.out.println("ENTER ProcessSiteQuery");

		String userID = (String) commandObj.get("user");
		String custID = (String) commandObj.get("set_name");

		try {
			Key userKey = new Key(this.namespace, custID, userID);
			String siteListBin = "Site List";

			// Initialize large List operator.
			com.aerospike.client.large.LargeList llist = client.getLargeList(this.policy, userKey, siteListBin, null);

			// Perform a Scan on all of the Site Visit Objects
			List<Map<String,Object>> scanList =  (List<Map<String,Object>>) llist.scan();
			for (Map<String,Object> mapItem : scanList) {
				console.info("Map Item" + mapItem );
			}

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		System.out.println("Done with Site Query");
	} // end processSiteQuery()
	
	/**
	 * Use the Range Query capability of LLIST to find all values between
	 * MIN and Expire.  Then use that result list (if any qualify) as the list
	 * of items to REMOVE from the list.
	 * Note that a value of NIL in the range will start searching at the LEAST
	 * (i.e. the leftmost) item.
	 * 
	 * @param commandObj
	 * @param params
	 */
	public void processRemoveExpired( JSONObject commandObj  ) {
		System.out.println("ENTER ProcessRemoveExpired");

		String userID = (String) commandObj.get("user");
		String custID = (String) commandObj.get("set_name");
		Long expireLong = (Long) commandObj.get("expire");

		try {
			Key userKey = new Key(this.namespace, custID, userID);
			String siteListBin = "Site List";

			// Initialize large List operator.
			com.aerospike.client.large.LargeList llist = client.getLargeList(this.policy, userKey, siteListBin, null);

			// Perform a Range Query -- from "MIN" to "EXPIRE"
			Value minValue = new Value.NullValue();
			Value maxValue = Value.get(expireLong);

			List<Map<String,Object>> rangeList =  (List<Map<String,Object>>) llist.range( minValue, maxValue );

			// Process all items that are returned from the range query
			for (Map<String,Object> mapItem : rangeList) {
				System.out.println("Map Item" + mapItem );
			}

			// For each item in the range query, remove that item from the 
			// Large List.
			for (Map<String,Object> mapItem : rangeList) {
				System.out.println("Removing Map Item(" + mapItem + ") From the LLIST." );
				llist.remove(Value.getAsMap(mapItem));
			}	

			System.out.println("Checking Results after a REMOVE EXPIRE");
			// Validate Results with a Scan:
			List<Map<String,Object>> scanList =  (List<Map<String,Object>>) llist.scan();
			if (scanList.size() > 0 ) {
				console.info("Showing Remaining Items after Expire.");
				for (Map<String,Object> mapItem : scanList) {
					console.info("Map Item" + mapItem );
				}
			} else {
				console.info("NO Objects from Scan: Nothing left after Expire.");
			}

		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e);
		}
		console.info("Done with Remove Expired");
	} // processRemoveExpired()

} // end class LListOperations