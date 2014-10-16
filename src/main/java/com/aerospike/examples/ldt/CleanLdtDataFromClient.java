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
import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

/**
 * This class represents a thread of execution that performs client-side LDT
 * CLEAN operations on the User-Site Data in a particular set.  The thread
 * sleeps a specified amount of time, then wakes up and scans its set.  For each
 * record in the set, it performs a CLEAN operation on the LDT field in the 
 * record. The CLEAN operation removes all LDT data items that are older than a 
 * given value.
 * 
 * @author toby
 *
 */
public class CleanLdtDataFromClient implements Runnable {

	private Console console;  	// debug tracking/printing
	private DbOps dbOps;		// Link to DB and LDT Operations
	private AerospikeClient client; // Active Aerospike Client
	private String namespace;	// Aerospike DB Namespace
	private int threadNumber;	// Number of this thread instance
	private int setNum;			// Seed value for this customer set
	private int sleepInterval;  // Time (in seconds) to sleep between scans
	private long runPeriod;	// Number of nanoseconds we want this thread to run
	


	/**
	 * Initilize the constructor.
	 * @param console
	 * @param client
	 * @param dbOps
	 * @param namespace
	 * @param setNum
	 * @param sleepInterval
	 * @param runSeconds
	 * @param threadNumber
	 */
	public CleanLdtDataFromClient(Console console, AerospikeClient client, DbOps dbOps,
			String namespace,  int setNum, int sleepInterval, long runSeconds,
			int threadNumber ) 
	{
		this.console = console;
		this.client = client;
		this.dbOps = dbOps;
		this.namespace = namespace;
		this.threadNumber = threadNumber;
		this.client = client;
		this.setNum = setNum;
		this.sleepInterval = sleepInterval;
		this.runPeriod = runSeconds * 1000000000; // convert to nanoseconds
	}

	/**
	 * For "iteration" number of cycles, generate user-visit events within
	 * the bounds of customer range and user range.
	 */
	public void run() {
//		Random random = new Random();
//		int customerSeed = 0;
//		int userSeed = 0;
		String ns = namespace;
		Long expire = 0L;
//		int i = 0;
//		UserRecord userRec = null;
//		SiteVisitEntry sve = null;
		ILdtOperations ldtOps = dbOps.getLdtOps();

		CustomerRecord custRec = new CustomerRecord(console, setNum);
		String set = custRec.getCustomerID();

		// Remember that these times are in NANO-SECONDS
		long startTimeNs = System.nanoTime();
		long currentTimeNs;

		List<Key> keyList; // The list of record keys in this set.
		
		console.debug("Thread(" + threadNumber +") Starting");

		try {

			// Start off sleeping for the given interval, then wake up and
			// scan the set, get all of the record keys/digests, then open up
			// each LDT and remove any items that have expire times that are
			// OLDER than the current time.

			Thread.sleep(threadNumber * 2000); // stagger initial sleep.
			
			do { // Loop until time runs out.

				console.debug("Thread(" + threadNumber +") Running");

				// Scan this set, and for each record in the set, clean the
				// record.
				ScanKeySet scanKeySet = new ScanKeySet( console );
				keyList = scanKeySet.runScan(client, this.namespace, set);

				// Process all records (via keys) in the scanSet.
				for (Key key : keyList) {
					console.debug("Key:: " + key );
					expire = System.nanoTime();
					ldtOps.processRemoveExpired(ns, set, key, expire);
				}

				// Take a rest.  When we wake up see if our time is up.
				console.debug("Thread(" + threadNumber +") Sleeping");
				Thread.sleep(sleepInterval * 1000);
				currentTimeNs = System.nanoTime();
				if (currentTimeNs - startTimeNs > runPeriod) {
					console.info("All Done with Thread:" + threadNumber);
					break;
				}
			} while( true );
		} catch (Exception e) {
			e.printStackTrace();
			console.error("Problem with Thread(%d) ", threadNumber);
		}

	} // end run()
	
} // end class CleanLdtDataFromClient
