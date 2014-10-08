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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.RegisterTask;

/**
 * This class represents a thread of execution that performs server-side LDT 
 * CLEAN operations on the User-Site Data in a particular set. This class uses
 * a server-side UDF to perform the operation.
 * 
 * The execution of this class is as follows:
 * The thread sleeps a specified amount of time, then wakes up and scans its 
 * customer set.  For each record in the set, it invokes a UDF that will perform
 * a CLEAN operation on the LDT field in the record, without bringing any data
 * to the client.
 * 
 * The CLEAN operation removes all LDT data items that are older than a given
 * value.
 * 
 * @author toby
 *
 */
public class CleanLdtDataWithUDF implements Runnable, IAppConstants {

	private Console console;  	// debug tracking/printing
	private DbOps dbOps;		// Link to DB and LDT Operations
	private AerospikeClient client; // Active Aerospike Client
	private DbParameters parms; // Aerospike DB Parameters
	private int setNumber;		// The number that derives the Customer SetID.
	private int threadNumber;	// Number of this thread instance
	private int cleanIntervalSec;  // Time (in seconds) to sleep between scans
	private long runPeriodNs;	// Number of nanoseconds we want this thread to run

	/**
	 * Initialize the constructor.
	 * @param console
	 * @param client
	 * @param dbOps
	 * @param parms
	 * @param setNum
	 * @param cleanIntervalSec :: Amount to sleep between cleaning operations
	 * @param cleanDurationSec :: Total amount of time for clean thread lifetime
	 * @param threadNumber
	 */
	public CleanLdtDataWithUDF(Console console, AerospikeClient client, 
			DbOps dbOps, DbParameters parms, int setNum, int cleanIntervalSec, 
			long cleanDurationSec, int threadNumber ) 
	{
		this.console = console;
		this.client = client;
		this.dbOps = dbOps;
		this.parms = parms;
		this.setNumber = setNum;
		this.threadNumber = threadNumber;
		this.client = client;
		this.cleanIntervalSec = cleanIntervalSec;
		this.runPeriodNs = cleanDurationSec * 1000000000; // convert to nanoseconds
	}
	
	/**
	 * Perform the Aerospike Scan, with the UDF that will find and clean the
	 * expired SiteVisit Data entries.
	 * 
	 * @param client
	 * @param parms
	 * @param customerSet
	 * @param expire
	 * @throws Exception
	 */
	private void runScanUDF(
			AerospikeClient client,
			DbParameters parms,
			String customerSet,
			long expire
		) throws Exception 
	{			
			console.debug("<[ C L E A N  With  S C A N   U D F ]> For ns=%s set=%s expire=%d",
				parms.namespace, customerSet, expire);			
			
			Statement stmt = new Statement();
			stmt.setNamespace(parms.namespace);
			stmt.setSetName(customerSet);
			
			String moduleName = CM_LLIST_MOD;
			String functionName = LDT_EXPIRE;
			
			ExecuteTask task = client.execute(parms.policy, stmt, 
					moduleName, functionName, Value.get(dbOps.ldtBinName),
					Value.get(expire));
			task.waitTillComplete();
		}

	/**
	 * For "iteration" number of cycles, generate user-visit events within
	 * the bounds of customer range and user range.
	 */
	public void run() {
		long expireNs; 					// Expire time in nanoseconds
		long currentTimeNs;               // Current Time in nanoseconds
		long startTimeNs = System.nanoTime(); // Start time in nanoseconds
		
		console.info("Starting SCAN UDF RUN: SetNum(%d)", this.setNumber);

		try {

			// Start off sleeping for the given interval, then wake up and
			// scan the set.  The scan call will include a UDF that will, 
			// for each record, open the record, check for an existing 
			// site-visit LDT, (if present) scan the site-visit LDT and then
			// remove entries that are older than the supplied expire value.
			Thread.sleep(threadNumber * 1000); // stagger initial sleep.
			
			// Create a Customer Object so that we can get the customer Set Name.
			CustomerRecord crec = new CustomerRecord(console,setNumber);
			String custSet = crec.getCustomerID();
			
			do { // Loop until time runs out (runPeriodNs)

				console.debug("Clean Thread(" + threadNumber +") Running");

				// Scan this set and call the UDF on each record.  We get the
				// current time in nanoseconds, and any LDT item that has an
				// expire time older than current time will be removed.
				expireNs = System.nanoTime();
				runScanUDF(client, parms, custSet, expireNs);

				// Take a rest.  When we wake up see if our time is up.
				console.debug("Clean Thread(" + threadNumber +") Sleeping");
				Thread.sleep(cleanIntervalSec * 1000);
				currentTimeNs = System.nanoTime();
				console.debug("Testing CurrTime("+ currentTimeNs +") " +
						"StartTime(" + startTimeNs + ") " +
						"Diff(" + (currentTimeNs - startTimeNs) + ") " +
						"RunPeriod(" + runPeriodNs + ")");
				if (currentTimeNs - startTimeNs > runPeriodNs) {
					console.info("All Done with Thread:" + threadNumber);
					break;
				}
			} while( true );

		} catch (Exception e) {
			e.printStackTrace();
			console.error("Problem with Thread(%d)", threadNumber);
		}

	} // end run()
	
} // end class CleanLdtDataFromClient
