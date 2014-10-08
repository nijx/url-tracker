package com.aerospike.examples.ldt;

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
public class LListOperations implements ILdtOperations, IAppConstants {
	private AerospikeClient client;
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
	public LListOperations(AerospikeClient client, Console console) 
			throws AerospikeException 
	{

		this.client = client;
		this.console = console;
		this.writePolicy = new WritePolicy();
		this.writePolicy.timeout = 1000;
		this.writePolicy.maxRetries = 0;
		this.policy = new Policy();
	}
	
	public void setup() {
		// Nothing needed for LLIST (yet).
		// Eventually, we will register needed UDFs here.
	}
	
	/**
	 * Create a MAP object that will hold the Site Visit value.
	 * @param entry
	 * @return
	 */
	public Map<String,Object> newSiteObject(SiteVisitEntry entry) {

		// For THIS example, we're going to put the Site Visit object into
		// a Large List (LLIST), so we're going to use as an Object Key the
		// Expire Date (an integer).  That way, the Site Visit objects will
		// be kept in EXPIRE ORDER, and thus will be easy to scan and manage.
		HashMap<String,Object> siteObjMap = new HashMap<String,Object>();
		siteObjMap.put("key", entry.getExpire());
		siteObjMap.put("name", entry.getUserID());
		siteObjMap.put("URL", entry.getUrl());
		siteObjMap.put("referrer", entry.getReferrer());
		siteObjMap.put("page_title", entry.getPageTitle());
		siteObjMap.put("date", entry.getDate());
		
		return siteObjMap;	
	}
	
	/**
	 * Enter a new Site Visit object in the collection of site visits for
	 * a particular user.  Order the Site Visit Objects by Expire Time.
	 * @param commandObj
	 * @param params
	 * @return the status:  zero ok, -1 Gen error, -2 Duplicate Key (retry)
	 */
	public int storeSiteObject(SiteVisitEntry sve, String namespace,
			Map<String,Object> siteObjMap  ) 
	{
		console.debug("ENTER storeObject:");
		
		// The Customer ID (custID) is the Aerospike SET name, and userID is the
		// key for the record (the user data and the site visit list).
		String userID = sve.getUserID();
		String custID = sve.getCustID();

		try {		

			Key userKey = new Key(namespace, custID, userID);
			String siteListBin = sve.getLdtBinName();

			// Initialize Large LIST operator.
			com.aerospike.client.large.LargeList llist = 
					client.getLargeList(this.policy, userKey, siteListBin, 
							CM_LLIST_MOD);

			// Package up the Map Object and add it to the LLIST.  Note that the
			// "Value.get()" operation is NOT used.  Instead it's Value.getAsMap().
			llist.add(Value.getAsMap(siteObjMap));

		} catch (AerospikeException ae) {
			console.debug("DB Error:  Retry");
			return( -2 );
		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Store Site Visit Exception: " + e);
			return( -1 );
		}
		return(0);
	} // end storeSiteObject()


	/**
	 * Enter a new Site Visit object in the collection of site visits for
	 * a particular user.  Order the Site Visit Objects by Expire Time.
	 * @param commandObj
	 * @param params
	 */
	public void processNewSiteVisit( JSONObject commandObj, String ns  ) {
		console.debug("ENTER ProcessNewSiteVisit:");
		
		SiteVisitEntry sve = 
				new SiteVisitEntry(console, commandObj, ns, 0, UserTraffic.LDT_BIN );

		try {
			sve.toStorage(client, ns, this);		
		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Process New Site Visit Exception: " + e);
		}
	} // end processNewSiteVisit()


	/**
	 * Scan the user's Site Visit List.  Get back a List of Maps that we
	 * can peruse and print.
	 * @param commandObj
	 * @param params
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String,Object>> 
	processSiteQuery( String ns, String set, String key ) 
	{
		console.debug("ENTER ProcessSiteQuery: NS(%s), Set(%s) Key(%s)",
				ns, set, key);
		
		List<Map<String,Object>> scanList = null;

		try {
			Key userKey = new Key(ns, set, key);
			String siteListBin = UserTraffic.LDT_BIN;

			// Initialize large List operator.
			com.aerospike.client.large.LargeList llist = 
					client.getLargeList(this.policy, userKey, siteListBin, null);

			// Perform a Scan on all of the Site Visit Objects
			scanList =  (List<Map<String,Object>>) llist.scan();
			if( console.debugIsOn() ) {
				for (Map<String,Object> mapItem : scanList) {
					console.debug("ScanList Map Item" + mapItem );
				}
			}

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Process Site Query Exception: " + e);
		}
		console.debug("Done with Site Query");
		
		return scanList;
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
	public void processRemoveExpired( String ns, String set, Key key, long expire ) {
		console.debug("ENTER ProcessRemoveExpired");
		List<Map<String,Object>> scanList = null;

		try {
			String siteListBin = UserTraffic.LDT_BIN;

			// Initialize large List operator.
			com.aerospike.client.large.LargeList llist = 
					client.getLargeList(this.policy, key, siteListBin, null);

			// Perform a Range Query -- from "MIN" to "EXPIRE"
			Value minValue = new Value.NullValue();
			Value maxValue = Value.get(expire);

			try {
				List<Map<String,Object>> rangeList =  
						(List<Map<String,Object>>) llist.range( minValue, maxValue );

				// Process all items that are returned from the range query
				for (Map<String,Object> mapItem : rangeList) {
					console.debug("Map Item" + mapItem );
				}

				// For each item in the range query, remove that item from the 
				// Large List.
				for (Map<String,Object> mapItem : rangeList) {
					console.debug("Removing Map Item(" + mapItem + ") From the LLIST." );
					llist.remove(Value.getAsMap(mapItem));
				}
			} catch (AerospikeException ae) {
				// Ignore Aerospike Exception
				
			}
			
			// Remove this for the moment -- reactivate when we're checking
			// Post Expiration state.
//			if( console.debugIsOn() ) {
//				System.out.println("Checking Results after a REMOVE EXPIRE::" + expire);
//				// Validate Results with a Scan:
//				scanList = (List<Map<String,Object>>) llist.scan();
//				if (scanList.size() > 0 ) {
//					console.debug("Showing Remaining Items after Expire.");
//					for (Map<String,Object> mapItem : scanList) {
//						System.out.println("Map Item:: " + mapItem );
//					}
//				} else {
//					System.out.println("NO Objects from Scan: Nothing left after Expire.");
//				}
//			}

		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Process Remove Expired Exception: " + e);
		}
		console.debug("Done with Remove Expired");
	} // processRemoveExpired()

} // end class LListOperations