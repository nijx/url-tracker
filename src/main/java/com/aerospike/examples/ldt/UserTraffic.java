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
 * This class represents a thread of execution that performs operations on the
 * User-Site Data.  Once the Customer Set and the User Records have been set
 * up (as specified by the user's input parameters), one or more of these
 * threads will generate Site-Visit data (that simulates visits to URLs)
 * and inserts those entries into a Site-Visit collection (implemented by
 * an LDT) that is held in a User Record.
 * 
 * Along with the Site-Visit data generation, we periodically perform
 * administrative operations, such as the cleaning of expired data.
 * In some cases, we do "spot cleaning" (specify a particular record), and
 * in other cases we do a scan of all User Records of an entire Customer Set
 * and scrub (i.e. clean) each LDT collection and remove any entries that are
 * older than a given value.
 * 
 * @author toby
 *
 */
public class UserTraffic implements Runnable {

	private Console console;
	private DbOps dbOps;
	private AerospikeClient client;
	private int customerMax = 0;
	private int userMax = 0;
	private String namespace;
	int threadNumber;
	private int iterations = 0;


	public UserTraffic(Console console, AerospikeClient client, DbOps dbOps,
			String namespace, int iterations, int customers, int users,
			int threadNumber ) 
	{
		this.console = console;
		this.client = client;
		this.dbOps = dbOps;
		this.customerMax = customers;
		this.userMax = users;
		this.namespace = namespace;
		this.iterations = iterations;
		this.threadNumber = threadNumber;
		this.client = client;
	}

	/**
	 * For "iteration" number of cycles, generate user-visit events within
	 * the bounds of customer range and user range.
	 */
	public void run() {
		Random random = new Random();
		int customerSeed = 0;
		int userSeed = 0;
		String ns = namespace;
		String set = null;
		String keyStr = null;
		Long expire = 0L;
		int i = 0;
		int generateCount = this.iterations;
		CustomerRecord custRec = null;
		UserRecord userRec = null;
		SiteVisitEntry sve = null;
		ILdtOperations ldtOps = dbOps.getLdtOps();
		
		try {
			
			// Start a steady-State insertion and expiration cycle.
			// For "Generation Count" iterations, generate a pseudo-random
			// pattern for a Customer/User record and then insert a site visit
			// record.  Since we're using TIME (nano-seconds) for the key, we
			// expect that it will be unique.   If we do get a collision (esp
			// when we start using multiple threads to drive it), we'll just
			// retry (which will give us a different nano-time number).
			//
			// Also -- we will invoke multiple instances of the client, each
			// in a thread,  to increase the traffic to the DB cluster (and thus
			// giving it more exercise).
			//
			// New addition:  If our user has specified more than one thread,
			// then we'll fire off multiple threads

			
			console.info("Done with Load.  Starting Site Visit Generation.");
			for (i = 0; i < generateCount; i++) {
				customerSeed = random.nextInt(this.customerMax);
				custRec = new CustomerRecord(console, customerSeed);
				
				userSeed = random.nextInt(this.userMax);
				userRec = new UserRecord(console, custRec.getCustomerID(), userSeed);
				
				sve = new SiteVisitEntry(console, custRec.getCustomerID(), 
						userRec.getUserID(), i);
				sve.toStorage(client, namespace, ldtOps);
				
				set = custRec.getCustomerID();
				keyStr = userRec.getUserID();
				
				Key key = new Key(ns, set, keyStr);	
				
				// At predetermined milestones, perform various actions 
				if( i % 100 == 0 ) {
					console.info("ThreadNum(%d) Stored Cust#(%d) CustID(%s) User#(%d) UserID(%s) SVE(%d)",
							threadNumber, customerSeed, set, userSeed, keyStr, i);
				}
				if( i % 200 == 0 ) {
					console.info("ThreadNum(%d) QUERY: Stored Cust#(%d) CustID(%s) User#(%d) UserID(%s) SVE(%d)",
							threadNumber, customerSeed, set, userSeed, keyStr, i);
					dbOps.printSiteVisitContents(set, keyStr);
				}
//				if( i % 300 == 0 ) {
//					console.info("ThreadNum(%d) CLEAN: Stored Cust#(%d) CustID(%s) User#(%d) UserID(%s) SVE(%d)",
//							threadNumber, customerSeed, set, userSeed, keyStr, i);
//					expire = System.nanoTime();
//					ldtOps.processRemoveExpired( ns, set, key, expire );
//				}			
			} // end for each generateCount
			
		} catch (Exception e) {
			e.printStackTrace();
			console.error("Problem with Thread(%d) Customer Record: Seed(%d)", i);
		}

	} // end run()
	
} // end class UserTraffic
