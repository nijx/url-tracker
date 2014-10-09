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

import java.util.Map;

import org.json.simple.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class SiteVisitEntry {

	private Console console;
	private String custID; // The Set Name
	private String userID; // The User Record
	private String url;
	private String referrer;
	private String pageTitle;
	private long date;
	private long expire;
	private long timeToLive;
	private int    index;
	private String ldtBinName;
	private WritePolicy writePolicy = new WritePolicy();
	private Policy policy = new Policy();
	
	/**
	 * Generate a customer record based on the seed value. 
	 * @param console
	 * @param custID
	 * @param userID
	 * @param seed
	 * @param ldtBinName
	 * @param timeToLive -- time expressed in nanoseconds.
	 */
	public SiteVisitEntry(Console console, String custID, String userID, 
			int seed, String ldtBinName, long timeToLive) 
	{
		this.console = console;
		this.custID = custID;
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
		console.debug("Enter SiteVisitEntry");
		
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
		this.custID = custStr;
		
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
			console.error("Error Building Site Visit Entry");
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
			String userID, String referrer, String pageTitle, Long date, 
			Long expire, int seed, String ldtBinName)
	{
		this.console = console;	
		
		this.custID = custID;
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
	 * The chosen LDT is passed in (LLIST or LMAP).
	 * 
	 * @param client
	 * @param ldtOps
	 * @return
	 * @throws Exception
	 */
	public int toStorage(AerospikeClient client, String namespace,
			ILdtOperations ldtOps) 
			throws Exception 
	{
		int result = 0;
		int retryCount = 5;
		
		try {

			// Create a MAP object that will hold the Site Visit value.
			Map<String,Object> siteObjMap = ldtOps.newSiteObject(this);
			
			int i = 0;
			for (i = 0; i < retryCount; i++){
				// Store the Map Object in the appropriate LDT
				result = ldtOps.storeSiteObject(this, namespace, siteObjMap);
				if (result == 0){
					break;
				} else if (result == -2) {
					// let's retry.  First, refresh, then try again.
					console.debug("Storage Collision: Retry");
					Thread.sleep(10);  // Sleep ten milliseconds and try again.
					this.refreshSiteVisitEntry();
				} else {
					console.error("General Error on Site Visit Store");
					break;
				}
			}
			if (result != 0) {
				console.error("Failure Storing Object: ErrResult(%d) Retries(%d)",
						result, i);
			}

		} catch (Exception e){
			e.printStackTrace();
			console.error("Exception: " + e);
		}

		return result;
	}

//	/**
//	 * Given a User object, read it from the set (using the key custID) and
//	 * validate it with the manufactured object.
//	 * 
//	 * @param client
//	 * @param nameSpace
//	 * @return
//	 * @throws Exception
//	 */
//	public SiteVisitEntry fromStorage(AerospikeClient client, 
//			String namespace, ILdtOperations ldtOps) 
//			throws Exception 
//	{
//		SiteVisitEntry result = this;
//
//		// Set is the Customer ID, Record Key is the userID.
//		String setName = this.custID;
//		String recordKey = this.userID;
//		
//		try {
//
//			// Get the User Record for a given UserId and CustID
//			Key key = new Key(namespace, setName, recordKey);
//			console.debug("Get: namespace(%s) set(%s) key(%s)",
//					key.namespace, key.setName, key.userKey);
//
//			// Read the record and validate.
//			Record record = client.get(this.policy, key);
//			if (record == null) {
//				throw new Exception(String.format(
//						"Failed to get: namespace=%s set=%s key=%s", 
//						key.namespace, key.setName, key.userKey));
//			}
//			
//			console.debug("Record Result:" + record );
//			
//
//		} catch (Exception e){
//			e.printStackTrace();
//			console.warn("Exception: " + e);
//		}
//		
//		return result;
//	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("UserID(%s)", userID));
		sb.append(String.format("CustID(%s)", custID));
		sb.append(String.format("URL(%s)", url));
		sb.append(String.format("Referrer(%s)", referrer));
		sb.append(String.format("PageTitle(%s)", pageTitle));
		sb.append(String.format("Date(%d)", date));
		sb.append(String.format("Expire(%d)", expire));
		sb.append(String.format("Index(%d)",  index));
		
		return sb.toString();
	}
	

	public String getCustID() {
		return custID;
	}

	public void setCustID(String custID) {
		this.custID = custID;
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

	public WritePolicy getWritePolicy() {
		return writePolicy;
	}

	public void setWritePolicy(WritePolicy writePolicy) {
		this.writePolicy = writePolicy;
	}

	public Policy getPolicy() {
		return policy;
	}

	public void setPolicy(Policy policy) {
		this.policy = policy;
	}

	public String getLdtBinName() {
		return ldtBinName;
	}

	public void setLdtBinName(String ldtBinName) {
		this.ldtBinName = ldtBinName;
	}
	
	
	
	
}
