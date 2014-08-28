package com.aerospike.examples.ldt;

//import java.io.Console;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;

/**
 * LMapOperations holds functions that manage the URL Site Visit data
 * with Aerospike Large Map.
 * 
 * Using a Large Map to hold Site Visit data means that there only one value
 * object per name object, so the user attribute that we choose to be the name
 * value will determine how the data is managed and how the data is kept.
 * If we were to pick referrer, or user-agent id, or IP Address, then we could
 * have only one of those.  If we want to keep ALL of the site-visit attributes,
 * then we should use the date field as the key (assuming that a millisecond
 * granularity field would be sufficient for uniqueness).
 * 
@author toby
*/
public class LMapOperations implements ILdtOperations {
	private AerospikeClient client;
	private WritePolicy writePolicy;
	private Policy policy;
	protected Console console;
	
	// For the sake of this example, we are hardcoding the path of the UDF
	// that we'll be using in the LMAP Scan Filter example.
	private String serverUdfPath = "lmap_scan_filter.lua";
	private String clientUdfPath = "udf/lmap_scan_filter.lua";
//	private String serverUdfPath = "udflib .lua";
//	private String clientUdfPath = "udf/udflib.lua";
	private String udfFilter     = "expire_filter";

	/**
	 * Constructor for LMAP OPERATION class.
	 * @param client
	 * @param namespace
	 * @param set
	 * @param console
	 * @throws AerospikeException
	 */
	public LMapOperations(AerospikeClient client, Console console) 
			throws AerospikeException 
	{

		this.client = client;
		this.writePolicy = new WritePolicy();
		this.writePolicy.timeout = 1000;
		this.writePolicy.maxRetries = 0;
		this.policy = new Policy();
		this.console = console;
	} // end LMapOperations() constructor
	
	/**
	 * Register the UDF (used for doing scan filters of the site visit map)
	 * once before we start the regular operations.
	 */
	public void setup() {
//		try {
//		registerFilterUDF(clientUdfPath, serverUdfPath );
//		} catch (Exception e){
//			e.printStackTrace();
//			console.error("Error Registering UDF: " + e );
//		}
	} // end setup()
	
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
	 */
	public void storeSiteObject(SiteVisitEntry sve, String ns,
			Map<String,Object> siteObjMap  ) 
	{
		console.info("ENTER storeObject:");

//		// Extract the values from the JSON object
//		String nameStr = (String) sve.getUserID();
//		String customerStr = (String) sve.getCustID();
//		
//		// The Customer ID (custID) is the Aerospike SET name, and userID is the
//		// key for the record (the user data and the site visit list).
//		String userID = nameStr;
//		String custID = customerStr;
//
//		try {
//			
//			sve.toStorage(client, this);
//
//			Key userKey = new Key(this.namespace, custID, userID);
//			String siteListBin = "Site List";
//
//			// Initialize large set operator.
//			com.aerospike.client.large.LargeList llist = 
//					client.getLargeList(this.policy, userKey, siteListBin, null);
//
//			// Package up the Map Object and add it to the LLIST.  Note that the
//			// "Value.get()" operation is NOT used.  Instead it's Value.getAsMap().
//			llist.add(Value.getAsMap(siteObjMap));			
//
//		} catch (Exception e){
//			e.printStackTrace();
//			System.out.println("Exception: " + e);
//		}
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
				new SiteVisitEntry(console, commandObj, ns, 0);

		try {
			sve.toStorage(client, ns, this);		
		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e);
		}
	} // end processNewSiteVisit()

//	/**
//	 * Enter a new Site Visit object in the collection of site visits for
//	 * a particular user.  Manage the Site Visit Objects (in LMAP)
//	 *  by Expire Time.
//	 * @param commandObj
//	 * @param params
//	 */
//	public void processNewSiteVisit( JSONObject commandObj  ) {
//		console.info("ENTER ProcessNewSiteVisit:");
//
//		JSONObject siteObj = (JSONObject) commandObj.get("visit_info");
//
//		// Extract the values from the JSON object
//		String nameStr = (String) siteObj.get("user_name");
//		String urlStr = (String) siteObj.get("url");
//		String refStr = (String) siteObj.get("referrer");
//		String pageStr = (String) siteObj.get("page_title");
//		Long dateLong = (Long) siteObj.get("date");
//		Long expireLong = (Long) siteObj.get("expire");
//		String customerStr = (String) siteObj.get("set_name");
//		
//		// The Customer ID (custID) is the Aerospike SET name, and userID is the
//		// key for the record (the user data and the site visit list).
//		String userID = nameStr;
//		String custID = customerStr;
//
//		try {
//
//			Key userKey = new Key(this.namespace, custID, userID);
//			String siteMapBin = "Site Map";
//
//			// Create a MAP object that will hold the Site Visit value.
//			// For THIS example, we're going to put the Site Visit object into
//			// a Large Map (LMAP), so we're going to use as the LMAP Name value
//			// the Expire Date (an integer).  When it comes time to find a
//			// expire values that are past a deadline, we will SCAN the entire 
//			// LMAP and use a UDF Filter to locate those site visit maps that
//			// contain expired values.
//			HashMap<String,Object> siteObjMap = new HashMap<String,Object>();
//			siteObjMap.put("key", expireLong);
//			siteObjMap.put("name", nameStr);
//			siteObjMap.put("URL", urlStr);
//			siteObjMap.put("referrer", refStr);
//			siteObjMap.put("page_title", pageStr);
//			siteObjMap.put("date", dateLong);
//
//			// Initialize the large map (LMAP) operator.
//			com.aerospike.client.large.LargeMap lmap = 
//					client.getLargeMap(this.policy, userKey, siteMapBin, null);
//
//			// Package up the new NAME and VALUE pair and add them to the LMAP.  
//			// The NAME is the expire number and the VALUE is the SiteObject Map.
//			// Note that the "Value.get()" operation is NOT used to construct
//			// a Value that holdes a map.  Instead it's Value.getAsMap().		
//			Value nameValue = Value.get(expireLong);
//			Value mapValue = Value.getAsMap(siteObjMap);
//	
//			// Write the name/value pair into the LMAP holding site-visits.
//			lmap.put(nameValue, mapValue);			
//
//		} catch (Exception e){
//			e.printStackTrace();
//			System.out.println("Exception: " + e);
//		}
//	} // end processNewSiteVisit()


	/**
	 * Scan the user's Site Visit List.  Get back a List of Maps that we
	 * can peruse and print.
	 * @param commandObj
	 * @param params
	 */
	public List<Map<String,Object>> processSiteQuery( String ns, String set, String key ) {
		System.out.println("ENTER ProcessSiteQuery");
		
		// Even though we get the results as a large map, we have to return the
		// objects as a list.
		List<Map<String,Object>> resultList = null;

		try {
			Key userKey = new Key(ns, set, key);
			String siteMapBin = "Site Map";

			// Initialize large map operator.
			com.aerospike.client.large.LargeMap lmap = 
					client.getLargeMap(this.policy, userKey, siteMapBin, null);

			// Perform a Scan on all of the Site Visit Objects.  We get back
			// a large map, which we'll iterate thru.
			Map<Long, Map<String,Object>> mapResult =  
					(Map<Long, Map<String,Object>>) lmap.scan();
			for (Entry<Long,Map<String,Object>> entry : mapResult.entrySet() ){
				Long expireValue = (Long) entry.getKey();
				Map<String,Object> siteObj = (Map<String,Object>) entry.getValue();
				console.info("Site Entry: Expire(%d); SiteObj(%s)", expireValue, siteObj.toString());		
			}

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		System.out.println("Done with Site Query");
		
		return resultList;
	} // end processSiteQuery()
	
	/**
	 * Scan the LMAP and retrieve those entries that are beyond the expire range.
	 * 
	 * @param commandObj
	 * @param params
	 */
	public void processRemoveExpired(  String ns, String set, String key, long expire ) {
		System.out.println("ENTER ProcessRemoveExpired");

		try {
			Key userKey = new Key(ns, set, key);
			String siteMapBin = "Site Map";

			// Initialize large List operator.
			com.aerospike.client.large.LargeMap lmap = 
					client.getLargeMap(this.policy, userKey, siteMapBin, null);

			// Perform a full scan of ALL of the map objects, but employ a
			// UDF filter to find ONLY those objects that have expired.
			Value expireCutoffValue = Value.get(expire);
			Value udfFilterValue = Value.get(udfFilter);
			Map<Long,Map<String,Object>> expireMap =  
					(Map<Long,Map<String,Object>>) lmap.filter( serverUdfPath, udfFilterValue, expireCutoffValue );
			
			// Process the resultMap that comes back ("expireMap"), and show
			// what's in there.  ALSO, we remove those items that qualify for
			// "expiration".
			Value expireValue;
			for (Entry<Long,Map<String,Object>> entry : expireMap.entrySet() ){
				// First, print the values
				Long readExpireLong = (Long) entry.getKey();
				Map<String,Object> siteObj = (Map<String,Object>) entry.getValue();
				console.info("Site Entry: Expire(%l); SiteObj(%s)", 
						readExpireLong, siteObj.toString());	
				
				// Second, remove those values from the LMAP.
				expireValue = Value.get( readExpireLong );
				lmap.remove( expireValue );
			}

			System.out.println("Checking Results after a REMOVE EXPIRE");
			// Validate Results with a Scan:
			
			// Perform a Scan on all of the Site Visit Objects.  We get back
			// a large map, which we'll iterate thru.
			Map<Long, Map<String,Object>> mapResult =  
					(Map<Long, Map<String,Object>>) lmap.scan();
			if ( mapResult.size() > 0 ) {
				console.info("Showing Scan Result");
				for (Entry<Long,Map<String,Object>> entry : mapResult.entrySet() ){
					expire = (Long) entry.getKey();
					Map<String,Object> siteObj = (Map<String,Object>) entry.getValue();
					console.info("Site Entry: Expire(%l); SiteObj(%s)", 
							expire, siteObj.toString());		
				}
			} else {
				console.info("LMAP Scan Result is EMPTY");
			}

		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e);
		}
		console.info("Done with Remove Expired");
	} // processRemoveExpired()
	
	/**
	 * Register the Filter UDF for our LMAP Scan.
	 * 
	 * Here's the Aerospike Definition
	 * public class AerospikeClient { 
	 *   public final RegisterTask register( Policy policy, String clientPath, 
	 *                String serverPath, Language language ) 
	 *                throws AerospikeException }
	 * 
	 * @param clientPath : String showing path to the UDF on the client side
	 * @param serverPath : String showing path to the UDF on the server side
	 * @throws Exception
	 */
	private void registerFilterUDF(String clientPath, String serverPath) throws Exception {
		RegisterTask task = client.register(this.policy,
				clientPath, serverPath, Language.LUA);
		task.waitTillComplete();
	} // end registerUDF()


} // end class LMapOperations