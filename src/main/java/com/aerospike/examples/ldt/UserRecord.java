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

public class UserRecord implements IDbRecord {

	private Console console;
	private String userID;
	private String email;
	private String phone;
	private String address;
	private String company;
	private String customerID;
	private int    index;
	private WritePolicy writePolicy = new WritePolicy();
	private Policy policy = new Policy();

	/**
	 * Generate a customer record based on the seed value.  This is used by
	 * the data generator.
	 * @param console
	 * @param seed
	 */
	public UserRecord(Console console, String custID, int seed) {

		this.userID = String.format("UserName(%d)", seed);
		this.email = String.format("Email(%d)", seed);
		this.phone = String.format("Phone(%08d)", seed);
		this.address = String.format("Address(%d)", seed);
		this.company = String.format("Company(%d)", seed);
		this.customerID = custID;
		this.index = seed; 
		this.console = console;

		console.debug("Creating Generated User Record(%d): CustID(%s) UserID(%s)",
				seed, custID, userID);
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
		this.customerID = custID;
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
		this.customerID = custID;
		this.index = seed; 
		this.console = console;
	}

	/**
	 * Take this customer object and write it to the Set.
	 */
	public int toStorage(AerospikeClient client, String namespace) throws Exception {
		console.debug("Enter toStorage(): NS(%s)", namespace);
		int result = 0;

		// Set is the Customer ID, Record Key is the userID.
		String setName = this.customerID;
		String recordKey = this.userID;

		try {

			// Note that custID is BOTH the name of the Aerospike SET and it
			// is the KEY of the Singleton Record for Customer info.
			Key key        = new Key(namespace, setName, recordKey);
			Bin custBin    = new Bin("custID", this.customerID);
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
		String setName = this.customerID;
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
	 * @return
	 * @throws Exception
	 */
	public Record  remove(AerospikeClient client, String namespace) 
			throws Exception 
	{
		Record record = null;

		// A slightly strange case where the customerID is BOTH the set name
		// and the Key for the customer record.
		String setName = this.customerID;
		String recordKey = this.userID;

		try {
			Key key        = new Key(namespace, setName, recordKey);

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
		sb.append(String.format("CustomerID(%s)", customerID));
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
		return customerID;
	}

	public void setCustomerID(String customerID) {
		this.customerID = customerID;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}


} // end UserRecord class.
