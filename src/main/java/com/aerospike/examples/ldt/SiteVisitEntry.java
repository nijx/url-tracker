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

import java.sql.Date;
import java.util.Map;

import org.json.simple.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class SiteVisitEntry {
	
	// Set TIME TO LIVE as 300 seconds (five minutes in nano-seconds).
	public static final long TIME_TO_LIVE = 300000000000L;

	private Console console;
	private String namespace;
	private String custID; // The Set Name
	private String userID; // The User Record
	private String url;
	private String referrer;
	private String pageTitle;
	private Long date;
	private Long expire;
	private int    index;
	private WritePolicy writePolicy = new WritePolicy();
	private Policy policy = new Policy();
	
	/**
	 * Generate a customer record based on the seed value.
	 * @param console
	 * @param seed
	 */
	public SiteVisitEntry(Console console, String namespace, String custID, 
			int seed) 
	{
		this.namespace = namespace;
		this.custID = custID;
		this.userID = String.format("UserName(%d)", seed);
		
		this.url = String.format("url(%d)", seed);
		this.referrer = String.format("Referrer(%d)", seed);
		this.pageTitle = String.format("PageTitle(%d)", seed);
		
		// Get the current Time.
		Date javaDate = new Date(0);
//		this.date = javaDate.getTime();
		this.date = System.nanoTime();
		this.expire = this.date + TIME_TO_LIVE;
		
		this.index = seed; 
	}
	
	/**
	 * Generate a Site Visit Entry based on the JSON VisitInfo Object.
	 * @param console
	 * @param seed
	 */
	public SiteVisitEntry(Console console, JSONObject commandObj, 
			String namespace, int seed) 
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
//		Long dateLong = Long.parseLong(dateStr);
		Long expireLong = (Long) siteObj.get("expire");
//		Long expireLong = Long.parseLong(expireStr);
		
		this.namespace = namespace;
		this.userID = nameStr;
		this.custID = custStr;
		
		this.url = urlStr;
		this.referrer = referrerStr;
		this.pageTitle = pageTitleStr;
		
		// Get the current Time.  Use this rather than the passed in time.
//		Date javaDate = new Date(0);
//		this.date = javaDate.getTime();
//		this.expire = this.date + TIME_TO_LIVE;
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
			Long expire, int seed )
	{
		this.console = console;	
		
		this.namespace = namespace;
		this.custID = custID;
		this.userID = userID;
		
		this.referrer = referrer;
		this.pageTitle = pageTitle;
		this.date = date;
		this.expire = expire;
		this.index = seed; 
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
	public int toStorage(AerospikeClient client, ILdtOperations ldtOps) 
			throws Exception 
	{
		int result = 0;
		
		try {

			// Create a MAP object that will hold the Site Visit value.
			Map<String,Object> siteObjMap = ldtOps.newSiteObject(this);
			
			// Store the Map Object in the appropriate LDT
			ldtOps.storeSiteObject(this, siteObjMap);		

		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Exception: " + e);
		}

		return result;
	}

	/**
	 * Given a User object, read it from the set (using the key custID) and
	 * validate it with the manufactured object.
	 * 
	 * @param client
	 * @param nameSpace
	 * @return
	 * @throws Exception
	 */
	public SiteVisitEntry  fromStorage(AerospikeClient client, ILdtOperations ldtOps) 
			throws Exception 
	{
		SiteVisitEntry result = this;

		// Set is the Customer ID, Record Key is the userID.
		String setName = this.custID;
		String recordKey = this.userID;
		
		try {

			// Get the User Record for a given UserId and CustID
			Key key        = new Key(namespace, setName, recordKey);
			console.debug("Get: namespace(%s) set(%s) key(%s)",
					key.namespace, key.setName, key.userKey);

			// Read the record and validate.
			Record record = client.get(this.policy, key);
			if (record == null) {
				throw new Exception(String.format(
						"Failed to get: namespace=%s set=%s key=%s", 
						key.namespace, key.setName, key.userKey));
			}
			
			console.debug("Record Result:" + record );
			

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		
		return result;
	}
	
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
	
	/**
	 * 	private Console console;
	private String namespace;
	private String custID; // The Set Name
	private String userID; // The User Record
	private String url;
	private String referrer;
	private String pageTitle;
	private Long date;
	private Long expire;
	private int    index;
	private WritePolicy writePolicy = new WritePolicy();
	private Policy policy = new Policy();
	 * @return
	 */
	

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
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
	
	
}
