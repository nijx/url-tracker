package com.aerospike.examples.ldt;

import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
//import java.io.Console;

/**
 * LDT Operations to be performed on Site-Visit Data.
 * 
@author toby
*/

public interface ILdtOperations {
	
	public static final String URL_FILLER =
			"http://www.url.com/_1234567890123456789012345678901234567890" +
			"123456789012345678901234567890123456789012345678901234567890";
	
	public static final String MISC_FILLER =
			"Miscellaneous_______1234567890123456789012345678901234567890" +
			"123456789012345678901234567890123456789012345678901234567890" +
			"123456789012345678901234567890123456789012345678901234567890";
	
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
	 * Return the size of the LDT in this record, in this bin.
	 * @param key
	 * @param bin
	 * @return
	 */
	public int  ldtSize(Key key, String bin);
	
	/**
	 * Store a Site Object into the LDT -- in the Base Customer Set.
	 * @param sve
	 * @param siteObjMap
	 */
	public int storeSiteObject(SiteVisitEntry sve, String ns, String set,
			Map<String,Object> siteObjMap);
	
//	/**
//	 * Store a Site Object into the LDT -- in the Cache Customer Set.
//	 * @param sve
//	 * @param namespace
//	 * @param siteObjMap
//	 * @return
//	 */
//	public int storeCachedSiteObject(SiteVisitEntry sve, String namespace,
//			Map<String,Object> siteObjMap  );
	
	
	/**
	 * Load up an entire LDT in the Segmented Cache with a Multi-Write.
	 * @param sve
	 * @param namespace
	 * @param fullLDT
	 * @return
	 */
	public int loadFullLDT(SiteVisitEntry sve, Key key,
			List<Map<String,Object>> fullLdtList  );

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
	
	/**
	 * Scan the user's Site Visit List, and return a list of MAP objects.
	 * @param ns
	 * @param set
	 * @param key
	 * @return
	 */
	public abstract List<Map<String,Object>> scanLDT( Key key) throws AerospikeException;

} // end interface ILdtOperations