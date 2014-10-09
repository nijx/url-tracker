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

import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

/**
 * This class represents a thread of execution that performs the load of
 * a Customer Set.  A Customer Set comprises a single Customer Record and a
 * number of User Records.  The Load Phase of the URL Tracker application
 * fires off independent threads -- one for each Customer Set.
 * 
 * @author toby
 *
 */
public class LoadCustomer implements Runnable, IAppConstants {

	private Console console;
	private AerospikeClient client;
	private int customerNumber;
	private String namespace;
	private int userRecords;

	public LoadCustomer(Console console, AerospikeClient client,
			String namespace, int customerNumber, int userRecords ) 
	{
		this.console = console;
		this.client = client;
		this.customerNumber = customerNumber;
		this.namespace = namespace;
		this.userRecords = userRecords;
		this.client = client;
	}

	/**
	 * For "iteration" number of cycles, generate user-visit events within
	 * the bounds of customer range and user range.
	 */
	public void run() {
		String customerSet = null;
		String keyStr = null;
		Long expire = 0L;
		int j = 0;
		CustomerRecord custRec = null;
		UserRecord userRec = null;
		
		console.info("Populate Customer(%d)", customerNumber);
		
		// Load up the Customer Record, then loop thru and load all of
		// The User Records.
		try {
			custRec = new CustomerRecord(console, customerNumber);
			custRec.toStorage(client, namespace);
			customerSet = custRec.getCustomerID();

			for (j = 0; j < userRecords; j++) {
				userRec = new UserRecord(console, customerSet, j);
				userRec.toStorage(client, namespace);
			} // end for each user record	
		} catch (Exception e) {
			e.printStackTrace();
			console.error("Problem with Customer Record(%d): User(%d)", 
					customerNumber, j);
		}

	} // end run()
	
} // end class LoadCustomer
