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

public class CustomerRecord {

	private Console console;
	private String customerName;
	private String contactName;
	private String customerID;
	private int    index;
	private WritePolicy writePolicy = new WritePolicy();
	private Policy policy = new Policy();
	
	/**
	 * Generate a customer record based on the seed value.
	 * @param console
	 * @param seed
	 */
	public CustomerRecord(Console console, int seed) {
		
		String name = String.format("CustName(%d)", seed);
		String id = String.format("CustID(%d)", seed);
		String contact = String.format("Contact(%d)", seed);
		
		this.console = console;
		this.customerName = name;
		this.contactName = contact;
		this.customerID = id; // doubles as the AS Set Name
		this.index = seed; // a fake number to help with generation
	}
	
	/**
	 * Generate a customer record based on the contents of a JSON object.
	 * @param console
	 * @param seed
	 */
	public CustomerRecord(Console console, JSONObject cmdObj, int seed) {
		
		JSONObject custObj = (JSONObject) cmdObj.get("customer");
		
		String customerStr = (String) custObj.get("customer_id");
		String contactStr = (String) custObj.get("contact");
		String custID = (String) custObj.get("set_name");
		
		this.console = console;
		this.customerName = customerStr;
		this.contactName = contactStr;
		this.customerID = custID; // doubles as the AS Set Name
		this.index = seed; // a fake number to help with generation
	}
	

	/**
	 * Generate a customer record based on real values.
	 * @param console
	 * @param name
	 * @param contact
	 * @param id
	 * @param index
	 */
	public CustomerRecord(Console console, String name, String contact, 
			String id, int index) 
	{
		this.console = console;
		this.customerName = name;
		this.contactName = contact;
		this.customerID = id; // doubles as the AS Set Name
		this.index = index; // a fake number to help with generation
	}

	/**
	 * Take this customer object and write it to the Set.
	 */
	public int toStorage(AerospikeClient client, String namespace) throws Exception {
		int result = 0;
		
		// A slightly strange case where the customerID is BOTH the set name
		// and the Key for the customer record.
		String setName = this.customerID;
		String recordKey = this.customerID;
		
		try {

			// Note that custID is BOTH the name of the Aerospike SET and it
			// is the KEY of the Singleton Record for Customer info.
			Key key        = new Key(namespace, setName, recordKey);
			Bin custBin    = new Bin("custID", customerID);
			Bin nameBin    = new Bin("name", customerName);
			Bin contactBin = new Bin("contact", contactName);

			console.debug("Put: namespace(%s) set(%s) key(%s) custID(%s) name(%s) contact(%s)",
					key.namespace, key.setName, key.userKey, custBin.value, nameBin.value, contactBin.value );

			// Write the Record
			client.put(this.writePolicy, key, custBin, nameBin, contactBin );

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
			result = -1;
		}

		return result;
	}

	/**
	 * Given a customer object, read it from the set (using the key custID) and
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

		// A slightly strange case where the customerID is BOTH the set name
		// and the Key for the customer record.
		String setName = this.customerID;
		String recordKey = this.customerID;
		
		try {

			// Note that custID is BOTH the name of the Aerospike SET and it
			// is the KEY of the Singleton Record for Customer info.
			Key key        = new Key(namespace, setName, recordKey);

			console.debug("Get: namespace(%s) set(%s) key(%s)",
					key.namespace, key.setName, key.userKey);

			// Read the record and validate.
			record = client.get(this.policy, key);
			if (record == null) {
				throw new Exception(String.format(
						"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
			}
			
			console.debug("Record Result:" + record );
			

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		
		return record;
	}
	
	/**
	 * Given a customer object, remove it from the set (using the key custID).
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
		String recordKey = this.customerID;
		
		try {
			// Note that custID is BOTH the name of the Aerospike SET and it
			// is the KEY of the Singleton Record for Customer info.
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
	 * Make our customer record more readable.
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("CustName(%s)", customerName));
		sb.append(String.format("Contact(%s)", contactName));
		sb.append(String.format("CustID(%s)", customerID));
		sb.append(String.format("Index(%d)",  index));
		
		return sb.toString();
	}

	public String getCustomerName() {
		return customerName;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public String getContactName() {
		return contactName;
	}

	public void setContactName(String contactName) {
		this.contactName = contactName;
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
	
	
}
