package com.aerospike.examples.ldt;

import java.util.Map;


//import java.io.Console;
import org.json.simple.JSONObject;

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
	public void storeSiteObject(SiteVisitEntry sve, Map<String,Object> siteObjMap);

	/**
	 * Enter a new Site Visit object in the collection of site visits for
	 * a particular user. 
	 * @param commandObj
	 */
	public abstract void processNewSiteVisit( JSONObject commandObj  );

	/**
	 * Scan the user's Site Visit List.  
	 * @param commandObj
	 */
	public abstract void processSiteQuery( JSONObject commandObj  );

	
	/**
	 * Remove expired site visit entries.
	 * 
	 * @param commandObj
	 */
	public abstract void processRemoveExpired( JSONObject commandObj  );


} // end interface ILdtOperations