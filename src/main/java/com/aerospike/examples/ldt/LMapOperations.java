package com.aerospike.examples.ldt;

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
 * then we should use the date field as the key (assuming that a micro-second
 * granularity field (or better) would be sufficient for uniqueness).
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
	 * a particular user.  Manage the Site Visit Objects by Expire Time.
	 * @param commandObj
	 * @param params
	 */
	public void storeSiteObject(SiteVisitEntry sve, String namespace,
			Map<String,Object> siteObjMap  ) 
	{
		console.debug("ENTER storeObject:");

		// The Customer ID (custID) is the Aerospike SET name, and userID is the
		// key for the record (the user data and the site visit list).
		String userID = (String) sve.getUserID();
		String custID = (String) sve.getCustID();
		
		try {

			Key userKey = new Key(namespace, custID, userID);
			String siteListBin = "Site List";

			// Initialize large MAP operator.
			com.aerospike.client.large.LargeMap lmap = 
					client.getLargeMap(this.policy, userKey, siteListBin, null);

			// Package up the Map Object and add it to the LMAP.  Note that the
			// "Value.get()" operation is NOT used.  Instead it's Value.getAsMap().
			lmap.put(Value.get(sve.getExpire()), Value.getAsMap(siteObjMap));			

		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e);
		}
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
		console.debug("ENTER ProcessSiteQuery");
		
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
				console.debug("Site Entry: Expire(%d); SiteObj(%s)", expireValue, siteObj.toString());		
			}

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.debug("Done with Site Query");
		
		return resultList;
	} // end processSiteQuery()
	
	
	/**
	 * Scan the LMAP and retrieve those entries that are beyond the expire range.
	 * 
	 * @param commandObj
	 * @param params
	 */
	public void processRemoveExpired(  String ns, String set, Key key, long expire ) {
		console.debug("ENTER ProcessRemoveExpired");

		try {
			String siteMapBin = "Site Map";

			// Initialize large List operator.
			com.aerospike.client.large.LargeMap lmap = 
					client.getLargeMap(this.policy, key, siteMapBin, null);

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
				console.debug("Site Entry: Expire(%l); SiteObj(%s)", 
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
				console.debug("Showing Scan Result");
				for (Entry<Long,Map<String,Object>> entry : mapResult.entrySet() ){
					expire = (Long) entry.getKey();
					Map<String,Object> siteObj = (Map<String,Object>) entry.getValue();
					console.debug("Site Entry: Expire(%l); SiteObj(%s)", 
							expire, siteObj.toString());		
				}
			} else {
				console.debug("LMAP Scan Result is EMPTY");
			}

		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e);
		}
		console.debug("Done with Remove Expired");
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