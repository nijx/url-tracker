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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;

/**
 * This class represents a thread of execution that performs the SCAN of
 * a Customer Set.  In each customer set, there are User Records that each
 * contain an LDT.  For EACH User Record, we will perform a full scan of the
 * LDT and process some statistics on it.
 * 
 * @author toby
 *
 */
public class ScanCustomer implements Runnable, IAppConstants {
	
	private Console console;  	// debug tracking/printing
	private DbOps dbOps;		// Link to DB and LDT Operations
	private AerospikeClient client; // Active Aerospike Client
	private String namespace;	// Aerospike DB Namespace
	private int threadNumber;	// Number of this thread instance
	private long customerNumber; // Number of the Customer Set

	
	/**
	 * Initialize Class with Constructor.
	 * @param console
	 * @param client
	 * @param dbOps
	 * @param namespace
	 * @param setNum
	 * @param sleepInterval
	 * @param runSeconds
	 * @param threadNumber
	 * @param customerNumber
	 */
	public ScanCustomer(Console console, AerospikeClient client, DbOps dbOps,
			String namespace,  int setNumber, int threadNumber ) 
	{
		this.console = console;
		this.client = client;
		this.dbOps = dbOps;
		this.namespace = namespace;
		this.threadNumber = threadNumber;
		this.customerNumber = setNumber;
		this.client = client;
	}

	/**
	 * For "iteration" number of cycles, generate user-visit events within
	 * the bounds of customer range and user range.
	 */
	public void run() {
		int j = 0;
		ILdtOperations ldtOps = dbOps.getLdtOps();
		List<Key> keyList; // The list of record keys in this set.
		List<Map<String,Object>> objectList;
		long maxLdtSize = 0L;
		long minLdtSize = 1000000L;
		long ldtCount = 0;
		long ldtAveSize = 0;
		long ldtSize = 0;
		long ldtTotalElementCount = 0;
		
		try {

			CustomerRecord custRec = new CustomerRecord(console, customerNumber);
			String customerSet = custRec.getCustomerID();
			console.debug("Scan Customer(%s)", customerSet);

			// Scan this set, and for each record in the set, Scan the entire LDT.
			ScanKeySet scanKeySet = new ScanKeySet( console );
			keyList = scanKeySet.runScan(client, this.namespace, customerSet);

			// Process all records (via keys) in the scanSet.
			for (Key key : keyList) {
				console.debug("Key:: " + key );
				try {
					objectList = ldtOps.scanLDT(key);
					if (objectList != null) {
						ldtSize = objectList.size();
						if (ldtSize > maxLdtSize) maxLdtSize = ldtSize;
						if (ldtSize < minLdtSize) minLdtSize = ldtSize;
						ldtCount++;
						ldtTotalElementCount += ldtSize;
					}
				} catch (AerospikeException ae) {
					// Ignore these for now.  It is most likely that the bin
					// does not exist in this record (which happens).
					console.error("Aerospike Error Code(%d) Error Message(%s)",
							ae.getResultCode(), ae.getMessage());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			console.error("Problem with Thread(%d) ", threadNumber);
		}
		
		// Print the final stats of this Customer Set Scan
		System.out.printf("Thread(%d) LDT Count(%d) MinSize(%d) MaxSize(%d) AveSize(%d)\n",
				threadNumber, ldtCount, minLdtSize, maxLdtSize, 
				(ldtTotalElementCount/ldtCount));

	} // end run()
	
} // end class ScanCustomer
