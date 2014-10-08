package com.aerospike.examples.ldt;

import java.util.List;
import java.util.Map;

import com.aerospike.client.Key;
//import java.io.Console;

/**
 * LDT Operations to be performed on Site-Visit Data.
 * 
@author toby
*/

public interface ILdtOperations {
	
	/**
	 * Initialize any necessary structures before regular command processing.
	 */
	public abstract void setup();
	
	/**
	 * Create an Object that will hold the Site Data.
	 * @param entry
	 * @return
	 */
	public Map<String,Object> newSiteObject(SiteVisitEntry entry);
	
	/**
	 * Store a Site Object into the LDT.
	 * @param sve
	 * @param siteObjMap
	 */
	public int storeSiteObject(SiteVisitEntry sve, String ns,
			Map<String,Object> siteObjMap);

	/**
	 * Scan the user's Site Visit List, and return a list of MAP objects.
	 * @param commandObj
	 */
	public abstract List<Map<String,Object>> processSiteQuery( String ns,
			String set, String key);

	/**
	 * Remove expired site visit entries that are older (smaller time) than
	 * the current time (expressed in nano-seconds).
	 * 
	 * @param ns
	 * @param set
	 * @param key
	 * @param expire
	 */
	public abstract void processRemoveExpired( String ns, String set, Key key,
			long expire);

} // end interface ILdtOperations