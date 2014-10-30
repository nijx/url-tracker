/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.examples.ldt;

import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;

public class SiteVisitEntry {

	private Console console;
	private String customerBaseSet; // The Set Name
	private String customerCacheSet;
	private String userID; // The User Record
	private String url;
	private String referrer;
	private String pageTitle;
	private long date;
	private long expire;
	private long timeToLive;
	private int    index;
	private String ldtBinName;
//	private WritePolicy writePolicy = new WritePolicy();
//	private Policy policy = new Policy();
	
	static final String CLASSNAME = "SiteVisitEntry";
	
	/**
	 * Generate a customer record based on the seed value. 
	 * @param console
	 * @param customerBaseSet
	 * @param userID
	 * @param seed
	 * @param ldtBinName
	 * @param timeToLive -- time expressed in nanoseconds.
	 */
	public SiteVisitEntry(Console console, String custID, String userID, 
			int seed, String ldtBinName, long timeToLive) 
	{
		this.console = console;
		this.customerBaseSet = custID;
		this.customerCacheSet = custID + ":cache";
		this.userID = userID;
		
		this.url = String.format("url(%d)", seed);
		this.referrer = String.format("Referrer(%d)", seed);
		this.pageTitle = String.format("PageTitle(%d)", seed);
		
		this.ldtBinName = ldtBinName;
		
		// Get the current Time.  However, better to use NANO-seconds rather
		// than milliseconds -- because we get duplicates with milliseconds.
		this.timeToLive = timeToLive;
		this.date = System.nanoTime();
		this.expire = this.date + timeToLive;
		
		this.index = seed; 
	}
	
	/**
	 * Generate a Site Visit Entry based on the JSON VisitInfo Object.
	 * @param console
	 * @param seed
	 */
	public SiteVisitEntry(Console console, JSONObject commandObj, 
			String namespace, int seed, String ldtBinName) 
	{
		final String meth = "SiteVisitEntry";
		console.debug("Enter<%s:%s> ", CLASSNAME, meth);
		
		try {
		
		// A JSON Site Entry has:
		// (1) a Set Name
		// (2) a User Name
		// (3) a SiteVisit Object
		String nameStr = (String) commandObj.get("user_name");
		String custStr = (String) commandObj.get("set_name");
		
		JSONObject siteObj = (JSONObject) commandObj.get("visit_info");
		String urlStr = (String) siteObj.get("url");
		String referrerStr = (String) siteObj.get("referrer");
		String pageTitleStr = (String) siteObj.get("page_title");
		Long dateLong = (Long) siteObj.get("date");
		Long expireLong = (Long) siteObj.get("expire");

		this.userID = nameStr;
		this.customerBaseSet = custStr;
		this.customerCacheSet = custStr + ":cache";
		
		this.url = urlStr;
		this.referrer = referrerStr;
		this.pageTitle = pageTitleStr;
		
		this.ldtBinName = ldtBinName;
		
		// Set the current Time and Expire values.
		this.date = dateLong;
		this.expire = expireLong;
		
		this.index = seed; 
		this.console = console;
		} catch (Exception e) {
			e.printStackTrace();
			console.error("<%s:%s> Error Building Site Visit Entry",
					CLASSNAME, meth);
		}
	}
	

	/**
	 * Generate a Site Visit record based on real values.
	 * @param console
	 * @param name
	 * @param contact
	 * @param id
	 * @param index
	 */
	public SiteVisitEntry(Console console, String namespace, String custID, 
			String customerBaseNS, String customerSegNS,
			String userID, String referrer, String pageTitle, Long date, 
			Long expire, int seed, String ldtBinName)
	{
		this.console = console;	
		
		this.customerBaseSet = custID;
		this.customerCacheSet = custID + ":cache";
		this.userID = userID;
		
		this.referrer = referrer;
		this.pageTitle = pageTitle;
		this.date = date;
		this.expire = expire;
		this.index = seed; 
		
		this.ldtBinName = ldtBinName;
	}
	
	/**
	 * Re-Generate a SiteVisit record based on an existing instance.  In this
	 * case, we need a NEW expire time (with a new clock value).
	 */
	public void refreshSiteVisitEntry() {
		// Refresh with the current Time.
		this.date = System.nanoTime();
		this.expire = this.date + this.timeToLive;
	}

	
	/**
	 * Take this Site Visit object and write it to the LDT for the user.
	 * This method is used for both the Base Storage and the Cache Storage
	 * The chosen LDT is passed in (LLIST or LMAP).
	 * 
	 * @param client
	 * @param ldtOps
	 * @return
	 * @throws Exception
	 */
	public int toStorage(AerospikeClient client, String namespace, 
			String set, ILdtOperations ldtOps) 
			throws Exception 
	{
		final String meth = "toStorage()";
		int result = 0;
		int retryCount = 5;
		
		try {

			// Create a MAP object that will hold the Site Visit value.
			Map<String,Object> siteObjMap = ldtOps.newSiteObject(this);
			
			int i = 0;
			for (i = 0; i < retryCount; i++){
				// Store the Map Object in the appropriate LDT
				result = ldtOps.storeSiteObject(this, namespace, set, siteObjMap);
				if (result == 0){
					break;
				} else if (result == -2) {
					// let's retry.  First, refresh, then try again.
					console.debug("Storage Collision: Retry");
					Thread.sleep(10);  // Sleep ten milliseconds and try again.
					this.refreshSiteVisitEntry();
					 siteObjMap = ldtOps.newSiteObject(this);
				} else {
					console.error("<%s:%s> General Error on Site Visit Store",
							CLASSNAME, meth);
					break;
				}
			}
			if (result != 0) {
				console.error("<%s:%s> Failure Storing Object: ErrResult(%d) Retries(%d)",
						CLASSNAME, meth, result, i);
			}

		} catch (Exception e){
			e.printStackTrace();
			console.error("<%s:%s> Exception(%s)", CLASSNAME, meth, e.toString());
		}

		return result;
	}

	/**
	 * Given a User object, Scan the LDT from the Base Set and use that data
	 * to load up the LDT in the Cache Set.
	 * 
	 * Use this opportunity to compare the Size with the Scan amount to verify
	 * that our data and LDT statistics are in sync.
	 * 
	 * @param client
	 * @param nameSpace
	 * @return
	 * @throws Exception
	 */
	public SiteVisitEntry reloadCache(AerospikeClient client, 
			String baseNamespace, String cacheNamespace, ILdtOperations ldtOps) 
			throws Exception 
	{
		
		final String meth = "reloadCache()";
		SiteVisitEntry result = this;

		// Set is the Customer ID, Record Key is the userID.
		String baseSetName = this.customerBaseSet;
		String cacheSetName = this.customerCacheSet;
		String recordKeyString = this.userID;

		int writeResult = 0;
		int sizeCheck = 0;
		int scanSize = 0;
		
		try {

			// First, we check the Segmented Cache for the UserRecord.
			// If it is not there, we will create it from the generated object
			// and the LDT info from the base customer set.

			// Get the User Record for a given UserId and CustID from the
			// Segmented Cache.
			Key baseKey = new Key(baseNamespace, baseSetName, recordKeyString);
			Key cacheKey = new Key(cacheNamespace, cacheSetName, recordKeyString);
			
			console.info("CCCCCCCCCCCCCCCCCCC Reload Cache LDT CCCCCCCCCCCCCCCCCCCCCCC");
			console.info("Base LDT: namespace(%s) set(%s) key(%s)",
					baseKey.namespace, baseKey.setName, baseKey.userKey);
			console.info("Cache LDT: namespace=%s set=%s key=%s", 
					cacheKey.namespace, cacheKey.setName, cacheKey.userKey);
			
			try {
				sizeCheck = ldtOps.ldtSize(baseKey, ldtBinName);
			} catch (AerospikeException ae) {
				console.info("<%s:%s> Error calling size(); on LDT",
						CLASSNAME, meth);
				console.error("Aerospike Error Code(%d) Error Message(%s)",
						ae.getResultCode(), ae.getMessage());
				sizeCheck = 0;
			} catch (Exception e) {
				// Do Nothing. keep on truckin.
				console.info("<%s:%s> General Error calling size(); on LDT",
						CLASSNAME, meth);
				sizeCheck = 0;
			}
			if (sizeCheck > 0) {
				List<Map<String,Object>> fullLdtList = ldtOps.scanLDT(baseKey);
				if (fullLdtList != null) {
					scanSize = fullLdtList.size();
				}
				// Validate the size.
				if (sizeCheck != scanSize) {
					console.error("<%s:%s> << SIZE MISMATCH: LDT Size(%d); Scan Size(%d) >>",
							CLASSNAME, meth, sizeCheck, scanSize);
				} else {
					console.info("<%s:%s> : LDT Size(%d); Scan Size(%d) >>",
							CLASSNAME, meth, sizeCheck, scanSize);
				}

				writeResult = ldtOps.loadFullLDT(this, cacheKey, fullLdtList);
				if (writeResult != 0){
					console.error("<%s:%s> Write Problem Loading LDT: RC(%d) namespace=%s set=%s key=%s", 
						CLASSNAME, meth, writeResult, 
						cacheKey.namespace, cacheKey.setName, cacheKey.userKey);
				}
			}
		} catch (Exception e){
			e.printStackTrace();
			console.error("<%s:%s> Exception(%s)", CLASSNAME, meth, e.toString());
		}
		
		return result;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("UserID(%s)", userID));
		sb.append(String.format("CustID(%s)", customerBaseSet));
		sb.append(String.format("URL(%s)", url));
		sb.append(String.format("Referrer(%s)", referrer));
		sb.append(String.format("PageTitle(%s)", pageTitle));
		sb.append(String.format("Date(%d)", date));
		sb.append(String.format("Expire(%d)", expire));
		sb.append(String.format("Index(%d)",  index));
		
		return sb.toString();
	}
	

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}

	public String getPageTitle() {
		return pageTitle;
	}

	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}

	public Long getDate() {
		return date;
	}

	public void setDate(Long date) {
		this.date = date;
	}

	public Long getExpire() {
		return expire;
	}

	public void setExpire(Long expire) {
		this.expire = expire;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

//	public WritePolicy getWritePolicy() {
//		return writePolicy;
//	}
//
//	public void setWritePolicy(WritePolicy writePolicy) {
//		this.writePolicy = writePolicy;
//	}
//
//	public Policy getPolicy() {
//		return policy;
//	}
//
//	public void setPolicy(Policy policy) {
//		this.policy = policy;
//	}

	public String getLdtBinName() {
		return ldtBinName;
	}

	public void setLdtBinName(String ldtBinName) {
		this.ldtBinName = ldtBinName;
	}

	public String getCustomerBaseSet() {
		return customerBaseSet;
	}

	public void setCustomerBaseSet(String customerBaseSet) {
		this.customerBaseSet = customerBaseSet;
	}

	public String getCustomerCacheSet() {
		return customerCacheSet;
	}

	public void setCustomerCacheSet(String customerCacheSet) {
		this.customerCacheSet = customerCacheSet;
	}
	
}
