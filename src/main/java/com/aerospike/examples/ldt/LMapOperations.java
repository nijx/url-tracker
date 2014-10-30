package com.aerospike.examples.ldt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * LMapOperations holds functions that manage the URL Site Visit data
 * with Aerospike Large Map.
 * 
 * NOTE:
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
public class LMapOperations implements ILdtOperations, IAppConstants {
	private AerospikeClient client;
	private Policy ldtPolicy;
	
	protected Console console;
	
	static final String CLASSNAME = "LMapOperations";

	/**
	 * Constructor for LMAP OPERATION class.
	 * @param client
	 * @param console
	 * @throws AerospikeException
	 */
	public LMapOperations(AerospikeClient client, Console console) 
			throws AerospikeException 
	{

		this.client = client;	
		this.ldtPolicy = new Policy();
		this.console = console;
	} // end LMapOperations() constructor
	
	/**
	 * Register the UDF (used for doing scan filters of the site visit map)
	 * once before we start the regular operations.
	 */
	public void setup() {
		// Do nothing for now.  Employ UDF Filters later.
	} // end setup()
	
	/**
	 * Return the size of the LDT in this record.
	 * @param namespace
	 * @param set
	 * @param key
	 * @param bin
	 * @return
	 */
	public int  ldtSize(Key key, String bin) {	
		int ldtSize = 0;
		try {		
			// Initialize Large LIST operator.
			com.aerospike.client.large.LargeMap lmap = 
					client.getLargeMap(this.ldtPolicy, key, bin, CM_LMAP_MOD);

			// Get the size.
			ldtSize = lmap.size();
		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Store Site Visit Exception: " + e);
		}
		return(ldtSize);	
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
		siteObjMap.put("URL", entry.getUrl() + URL_FILLER);
		siteObjMap.put("MISC1", MISC_FILLER);
		siteObjMap.put("MISC2", MISC_FILLER);
		siteObjMap.put("MISC3", MISC_FILLER);
		siteObjMap.put("MISC4", MISC_FILLER);
		siteObjMap.put("referrer", entry.getReferrer());
		siteObjMap.put("page_title", entry.getPageTitle());
		siteObjMap.put("date", entry.getDate());
		
		return siteObjMap;	
	}
	
	/**
	 * Enter a new Site Visit object in the collection of site visits for
	 * a particular user.  Manage the Site Visit Objects by Expire Time.
	 * @param sve: the SiteVisitEntry to store
	 * @param namespace: the aerospike namespace
	 * @param set: The Aerospike Set to use
	 * @param siteObjMap: The physical object to store
	 * @return the status:  
	 *   zero ok, 
	 *   -1 Gen error, 
	 *   -2 Duplicate Key (retry)
	 *   -3 Other Aerospike Error
	 */
	public int storeSiteObject(SiteVisitEntry sve, String namespace,
			String set, Map<String,Object> siteObjMap  ) 
	{
		final String meth = "storeSiteObject()";
		console.debug("ENTER<%s:%s>StoreSiteObject(%s) MapObj(%s)",
		  CLASSNAME, meth, sve.toString(), siteObjMap.toString());
		
		console.debug("ENTER<%s:%s> NS(%s) Set(%s)", CLASSNAME, meth, namespace, set);

		// The Customer ID (custID) is the Aerospike SET name, and userID is the
		// key for the record (the user data and the site visit list).
		String userID = (String) sve.getUserID();
		
		try {

			Key userKey = new Key(namespace, set, userID);
			String ldtBin = LDT_BIN;

			// Initialize large MAP operator.
			com.aerospike.client.large.LargeMap lmap = 
					client.getLargeMap(this.ldtPolicy, userKey, ldtBin,
							CM_LMAP_MOD);

			// Package up the Map Object and add it to the LMAP.  Note that the
			// "Value.get()" operation is NOT used.  Instead it's Value.getAsMap().
			lmap.put(Value.get(sve.getExpire()), Value.getAsMap(siteObjMap));			

		} catch (AerospikeException ae) {
			if (ae.getResultCode() == AS_ERR_UNIQUE) {
				// In this case, we want to retry (and not complain)
				return( -2 );
			} else {
				console.error("<%s:%s>Aerospike Error Code(%d) Error Message(%s)",
					CLASSNAME, meth, ae.getResultCode(), ae.getMessage());
				return( -3 );
			}
		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Store Site Visit Exception: " + e);
			return( -1 );
		}
		return(0);
	} // end storeSiteObject()
	
	/**
	 * Load up an entire LDT in the Segmented Cache with a Multi-Write.
	 * @param sve
	 * @param namespace
	 * @param fullLDT
	 * @return
	 */
	public int loadFullLDT(SiteVisitEntry sve, Key key, 
			List<Map<String,Object>> fullLdtList  ) 
	{
		final String meth = "loadFullLDT()";
		console.debug("ENTER<%s:%s> NS(%s) Set(%s) Key(%s)", CLASSNAME,
				meth, key.namespace, key.setName, key.userKey.toString());
		long expireTime;
		
		if (fullLdtList == null) {
			console.info("DEBUG: << FULL LDT LIST IS NULL >> !!!");
			return( -1 );
		}
		console.info("DEBUG: << FULL LDT >> " + fullLdtList.toString());
		
		try {		
			String siteListBin = sve.getLdtBinName();

			// Initialize Large MAP operator.
			com.aerospike.client.large.LargeMap lmap = 
				client.getLargeMap(this.ldtPolicy, key, siteListBin, CM_LLIST_MOD);

			// We're given a LIST of map objects, but for LMAP, we need to
			// turn it into one large Map, which means we really have a
			// Map of <expireTime, Map<String,Object>> objects.  Iterate thru
			// the list and build up the single large map for performing a
			// MULTI-WRITE into LMAP.
			Map<Long, Map<String,Object>> fullLdtMap = 
					new HashMap<Long, Map<String,Object>>();
			for (Map<String,Object> mapItem : fullLdtList) {
				expireTime = (Long) mapItem.get("key");
				fullLdtMap.put(expireTime, mapItem);
			}
			// Do a "one shot" write of the full map.
			lmap.put(fullLdtMap);

		} catch (AerospikeException ae) {
			console.debug("DB Error:  Retry");
			return( -2 );
		} catch (Exception e){
			e.printStackTrace();
			console.error("<%s:%s>Store Site Visit Exception(%s)",
					CLASSNAME, meth, e.toString());
			return( -1 );
		}
		return(0);
		
	} // end loadFullLDT()
	
	/**
	 * Enter a new Site Visit object in the collection of site visits for
	 * a particular user.  Order the Site Visit Objects by Expire Time.
	 * @param commandObj
	 * @param params
	 */
	public void processNewSiteVisit( JSONObject commandObj, String ns  ) {
		console.debug("ENTER ProcessNewSiteVisit:");
		
		SiteVisitEntry sve = 
				new SiteVisitEntry(console, commandObj, ns, 0, LDT_BIN);

		try {
			sve.toStorage(client, ns, sve.getCustomerBaseSet(), this);		
		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e);
		}
	} // end processNewSiteVisit()


	/**
	 * Scan the user's Site Visit List.  Get back a List of Maps that we
	 * can peruse and print.  Note, although this is currently similar to
	 * scanLDT(), the intention is that this method may evolve to do more
	 * complex actions, including filtering.
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
			String siteMapBin = LDT_BIN;

			// Initialize large map operator.
			com.aerospike.client.large.LargeMap lmap = 
					client.getLargeMap(this.ldtPolicy, userKey, siteMapBin, null);

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
	 * Scan the user's Site Visit List.  Get back a List of Maps that we
	 * can peruse and print.
	 * @param commandObj
	 * @param params
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String,Object>> scanLDT( Key key ) 
	throws AerospikeException 
	{
		console.debug("ENTER ScanLDT");
		
		// Even though we get the results as a large map, we have to return the
		// objects as a list.
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();

		try {
			String siteMapBin = LDT_BIN;

			// Initialize large map operator.
			com.aerospike.client.large.LargeMap lmap = 
					client.getLargeMap(this.ldtPolicy, key, siteMapBin, null);

			// Perform a Scan on all of the Site Visit Objects.  We get back
			// a large map, which we'll iterate thru.
			Map<Long, Map<String,Object>> mapResult =  
					(Map<Long, Map<String,Object>>) lmap.scan();
			if (mapResult != null) {
				
				console.info("LMAP SCAN: results(%s)", mapResult.toString());
				
				for (Entry<Long,Map<String,Object>> entry : mapResult.entrySet() ){
					Long expireValue = (Long) entry.getKey();
					Map<String,Object> siteObj = (Map<String,Object>) entry.getValue();
					console.debug("LMAP Site Entry: Expire(%d); SiteObj(%s)", expireValue, siteObj.toString());
					resultList.add(siteObj);
				}
			} 

		} catch (AerospikeException ae) {
			throw new AerospikeException(ae);
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.debug("Done with ScanLDT");
		
		return resultList;
	} // end scanLDT()
	
	
	/**
	 * Scan the LMAP and retrieve those entries that are beyond the expire range.
	 * 
	 * @param commandObj
	 * @param params
	 */
	public void processRemoveExpired(  String ns, String set, Key key, long expire ) {
		console.debug("ENTER ProcessRemoveExpired");

		try {
			String ldtBin = LDT_BIN;

			// Initialize large List operator.
			com.aerospike.client.large.LargeMap lmap = 
					client.getLargeMap(this.ldtPolicy, key, ldtBin, null);

			// Perform a full scan of ALL of the map objects
			Map<Long,Map<String,Object>> expireMap =  
					(Map<Long,Map<String,Object>>) lmap.scan();
			
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


} // end class LMapOperations