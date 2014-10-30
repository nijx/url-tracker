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

import org.json.simple.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class UserRecord implements IDbRecord, IAppConstants {

	private Console console;
	private String userID;
	private String email;
	private String phone;
	private String address;
	private String company;
	private String customerBaseSet;
	private String customerCacheSet;
	private int    index;
	private DbOps dbOps;
	private WritePolicy writePolicy;
	private WritePolicy cacheWritePolicy;
	private Policy policy;

	/**
	 * Generate a customer record based on the seed value.  This is used by
	 * the data generator.
	 * @param console
	 * @param seed
	 */
	public UserRecord(Console console, DbOps dbOps, String baseSet, int seed) {

		this.userID = String.format("UserName(%d)", seed);
		this.email = String.format("Email(%d)", seed);
		this.phone = String.format("Phone(%08d)", seed);
		this.address = String.format("Address(%d)", seed);
		this.company = String.format("Company(%d)", seed);
		this.customerBaseSet = baseSet;
		this.customerCacheSet = baseSet + ":cache";
		this.index = seed; 
		this.console = console;
		this.dbOps = dbOps;
		this.writePolicy = dbOps.writePolicy;
		this.cacheWritePolicy = dbOps.cacheWritePolicy;
		this.policy = dbOps.policy;
				

		console.debug("Generated User Record: Seed(%d): CustBase(%s) CustCache(%s) UserID(%s)",
				seed, customerBaseSet, customerCacheSet, userID);
	}

	/**
	 * Generate a customer record based on the JSON User Object.
	 * @param console
	 * @param seed
	 */
	public UserRecord(Console console, JSONObject cmdObj, int seed) {

		// JSON for User Record is Set Name, followed by User Object
		String custID = (String) cmdObj.get("set_name");

		JSONObject userObj = (JSONObject) cmdObj.get("user");
		String nameStr = (String) userObj.get("name");
		String emailStr = (String) userObj.get("email");
		String phoneStr = (String) userObj.get("phone");
		String addressStr = (String) userObj.get("address");
		String companyStr = (String) userObj.get("company");


		this.userID = nameStr;
		this.email = emailStr;
		this.phone = phoneStr;
		this.address = addressStr;
		this.company = companyStr;
		this.customerBaseSet = custID;
		this.index = seed; 
		this.console = console;

		console.debug("Creating JSON User Record(2): CustID(%s) UserID(%s)",
				custID, userID);
	}

	/**
	 * Generate a customer record based on real values.
	 * @param console
	 * @param name
	 * @param contact
	 * @param id
	 * @param index
	 */
	public UserRecord(Console console, String custID, String userID, String email, 
			String phone, String address, String company, int seed )
	{
		console.debug("Creating Explicit User Record(3): CustID(%s) UserID(%s)",
				custID, userID);

		this.console = console;		
		this.userID = userID;
		this.email = email;
		this.phone = phone;
		this.address = address;
		this.company = company;
		this.customerBaseSet = custID;
		this.index = seed; 
		this.console = console;
	}

	/**
	 * Take this User Record and write it to the Base Set.
	 */
	public int toStorage(AerospikeClient client, String namespace) throws Exception {
		console.debug("Enter toStorage(): NS(%s)", namespace);
		int result = 0;

		// Set is the Customer ID, Record Key is the userID.
		String setName = this.customerBaseSet;
		String recordKey = this.userID;

		try {

			// Note that custID is BOTH the name of the Aerospike SET and it
			// is the KEY of the Singleton Record for Customer info.
			Key key        = new Key(namespace, setName, recordKey);
			Bin custBin    = new Bin("custID", this.customerBaseSet);
			Bin nameBin    = new Bin("name", this.userID);
			Bin emailBin = new Bin("email", this.email);
			Bin phoneBin = new Bin("phone", this.phone);
			Bin addressBin = new Bin("address", this.address);
			Bin companyBin = new Bin("company", this.company);
			Bin indexBin = new Bin("index", this.index);

			console.debug("Put: namespace(%s) set(%s) key(%s) custID(%s) userID(%s)",
					key.namespace, key.setName, key.userKey, custBin.value, nameBin.value);

			console.debug("Put: Email(%s) phone(%s) addr(%s) company(%s) index(%s)",
					emailBin.value, phoneBin.value, addressBin.value, companyBin.value, indexBin.value);

			// Write the Record
			client.put(this.writePolicy, key, nameBin, emailBin, phoneBin,
					addressBin, companyBin, indexBin );

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}

		return result;
	} // end toStorage()
	
	/**
	 * If this User Record is not in the cache, then build a record and write
	 * it into the cache, along with a CACHE TTL (the cache entries expire
	 * regularly).
	 */
	public boolean updateCache(AerospikeClient client, String namespace) throws Exception {
		console.debug("Enter toStorage(): NS(%s)", namespace);
		boolean recordPresent = false;

		// Set is the Customer ID, Record Key is the userID.
		String cacheSetName = this.customerCacheSet;
		String recordKey = this.userID;
		Record record = null;

		try {
			Key key        = new Key(namespace, cacheSetName, recordKey);
			
			// First check to see if this record is present
			record = client.get(this.policy, key);
			if (record == null) {
				// Record is not in the cache, build a new one (from the old
				// information) and write it (with the CACHE TTL).
				Bin custBin    = new Bin("custID", this.customerBaseSet);
				Bin nameBin    = new Bin("name", this.userID);
				Bin emailBin = new Bin("email", this.email);
				Bin phoneBin = new Bin("phone", this.phone);
				Bin addressBin = new Bin("address", this.address);
				Bin companyBin = new Bin("company", this.company);
				Bin indexBin = new Bin("index", this.index);

				console.debug("Put: namespace(%s) set(%s) key(%s) custID(%s) userID(%s)",
						key.namespace, key.setName, key.userKey, custBin.value, nameBin.value);

				console.debug("Put: Email(%s) phone(%s) addr(%s) company(%s) index(%s)",
						emailBin.value, phoneBin.value, addressBin.value, companyBin.value, indexBin.value);

				// Write the Record
				client.put(this.cacheWritePolicy, key, nameBin, emailBin, phoneBin,
						addressBin, companyBin, indexBin );
//				record = client.get(this.policy, key);
//				console.info("JUST WROTE AND READ THIS RECORD: namespace(%s) set(%s) key(%s) In CACHE: Rec(%s)",
//						key.namespace, key.setName, key.userKey, record.toString());
				
			} else {
				console.debug("FOUND: namespace(%s) set(%s) key(%s) In CACHE: Rec(%s)",
						key.namespace, key.setName, key.userKey, record.toString());
				recordPresent = true;
			}

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}

		return recordPresent;
	} // end updateCache()

	/**
	 * Given a User object, read it from the set (using the key custID) and
	 * validate it with the manufactured object.
	 * 
	 * @param client
	 * @param nameSpace
	 * @return
	 * @throws Exception
	 */
	public Record  fromStorage(AerospikeClient client, String namespace) 
			throws Exception 
	{
		Record record = null;

		// Set is the Customer ID, Record Key is the userID.
		String setName = this.customerBaseSet;
		String recordKey = this.userID;

		try {

			// Get the User Record for a given UserId and CustID
			Key key        = new Key(namespace, setName, recordKey);
			console.debug("Get: namespace(%s) set(%s) key(%s)",
					key.namespace, key.setName, key.userKey);

			// Read the record and validate.
			record = client.get(this.policy, key);
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

		return record;
	} // end fromStorage()



	/**
	 * Given a User object, remove it from the set (using the key custID).
	 * 
	 * @param client
	 * @param nameSpace
	 * @param setName
	 * @return
	 * @throws Exception
	 */
	public Record  remove(AerospikeClient client, String namespace, String setName) 
			throws Exception 
	{
		Record record = null;
		String recordKey = this.userID;

		try {
			Key key = new Key(namespace, setName, recordKey);

			// Remove the record
			console.debug("Remove Record: namespace(%s) set(%s) key(%s)",
					key.namespace, key.setName, key.userKey);
			client.delete(this.writePolicy, key);

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}

		return record;
	} // end remove()


	/**
	 * 	Make our user record more readable.
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("UserID(%s)", userID));
		sb.append(String.format("Email(%s)", email));
		sb.append(String.format("Phone(%s)", phone));
		sb.append(String.format("Address(%s)", address));
		sb.append(String.format("Company(%s)", company));
		sb.append(String.format("CustomerID(%s)", customerBaseSet));
		sb.append(String.format("Index(%d)",  index));

		return sb.toString();
	}

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getCompany() {
		return company;
	}

	public void setCompany(String company) {
		this.company = company;
	}

	public String getCustomerID() {
		return customerBaseSet;
	}
	
	public String getCustomerBaseSet() {
		return customerBaseSet;
	}
	
	public String getCustomerCacheSet() {
		return customerCacheSet;
	}

	public void setCustomerID(String customerID) {
		this.customerBaseSet = customerID;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}


} // end UserRecord class.
