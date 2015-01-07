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
import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;

/**
 * This class represents a thread of execution that emulates User Behavior in
 * the URL-Tracker application.
 * One EmulateUser thread does the following:
 * (1) It writes a new entry into a Customer Set User Record URL Site Visit LDT.
 *     For tracking purposes, it also keeps a count of how many LDT updates
 *     have been performed (on that particular LDT).
 * (2) If that User Record is not in the Segmented Cache, then it reads the
 *     entire LDT from the Customer Set and builds a Segmented Cache Entry.
 *     Segmented Cache Entries have a TTL of one day (86400 seconds).
 * (3) It periodically scans LDTs and validates size information, as well as
 *     keeping statistics.
 *     
 * Once the Customer Set and the User Records have been set
 * up (as specified by the user's input parameters), one or more of these
 * threads will generate Site-Visit data (that simulates visits to URLs)
 * and inserts those entries into a Site-Visit collection (implemented by
 * an LDT) that is held in a User Record.
 * 
 * Along with the Site-Visit data generation, we periodically perform
 * administrative operations, such as:
 * (1) A Scan of an LDT to verify that the Size and Result Set match.
 * (2) An examination of the Full User Record to validate that the LDT Operations
 *     are consistent with the state of the User Record.
 * 
 * @author toby
 *
 */
public class EmulateUser implements Runnable, IAppConstants {

	private Console console;
	private DbOps dbOps;
	private AerospikeClient client;
	private int customerMax = 0;
	private int userMax = 0;
	private String baseNamespace;
	private String cacheNamespace;
	int threadNumber;
	private long emulationDays = 0;
	private long timeToLive;
	private int threadTPS;  // Number of transactions per second to run in this thread
	Random random;
	
	private static final String CLASSNAME = "EmulateUser";

	public EmulateUser(Console console, AerospikeClient client, DbOps dbOps,
			DbParameters dbParms, int threadTPS, long emulationDays, long customers, long users,
			int threadNumber, long timeToLive ) 
	{
		this.console = console;
		this.client = client;
		this.dbOps = dbOps;
		this.customerMax = (int) customers;
		this.userMax = (int) users;
		this.baseNamespace = dbParms.baseNamespace;
		this.cacheNamespace = dbParms.cacheNamespace;
		this.emulationDays = emulationDays;
		this.threadNumber = threadNumber;
		this.client = client;
		this.timeToLive = timeToLive;
		this.threadTPS = threadTPS;
		this.random = new Random();
	}
	
	/**
	 * For a given set of User Records (referred to by number), we're going 
	 * to perform a non-uniform random number allocation. We're going to do
	 * random for everyone, except the first N records.  The first N records
	 * will get 10x more operations than the remaining records.
	 * I was going to use a Poisson distribution, but I couldn't find any
	 * good examples.  Also, I have better control this way over the number
	 * of buckets and the percentage share of each.
	 * @param userRecordRange
	 */
	private long getUserRecordSeed(int userRecordRange) {
		// Experiment with these values -- try different numbers of special
		// records and different multiplier values
		int specialRecords = 10; // 10 special records per customer
		int multiplier = 4; // 4 times more likely to occur
		int superSpace = userRecordRange + (specialRecords * multiplier);
		
		int userSeed = 0;
		int randomResult = random.nextInt(superSpace);
		if (randomResult < userRecordRange) {
			userSeed = randomResult;
		} else {
			userSeed = (randomResult - userRecordRange) / multiplier;
		}
		
		return userSeed;
	}
	
	/**
	 * Do the main operation in emulate Mode:
	 * (1) Write a new URL Site Visit Record to the base DB
	 * (2) Check to see if the corresponding User Record is in the Segmented Cache
	 *     -- If not, write a new User Record with the CACHE TTL
	 *     -- UPDATE THE USER RECORD WITH A NEW (FULL) LDT Load
	 *        (Scan the existing LDT in the base DB and write to the cache LDT)
	 * (3) Probe the Base DB to see if the LDT data is consistent.
	 * 
	 * @param opNum
	 */
	private void doOperation(int opNum, ILdtOperations ldtOps){
		final String meth = "doOperation()";
		boolean recordPresent = false;
		String baseSet;
		String cacheSet;
		try {
			int customerSeed = random.nextInt(this.customerMax);
			CustomerRecord custRec = new CustomerRecord(console, customerSeed);

			long userSeed = getUserRecordSeed(this.userMax);
			UserRecord userRec = 
					new UserRecord(console, dbOps, custRec.getCustomerID(), (int) userSeed);

			SiteVisitEntry sve = new SiteVisitEntry(console, custRec.getCustomerID(), 
					userRec.getUserID(), opNum, LDT_BIN, this.timeToLive);
			
			baseSet = userRec.getCustomerBaseSet();
			cacheSet = userRec.getCustomerCacheSet();
			
			// Write the Site Visit to Storage -- which is hidden behind
			// this interface because there can be multiple implementations
			// of the LDT.
			sve.toStorage(client, baseNamespace, baseSet, ldtOps);

			// Check to see if the UserRecord is in the Segment Cache.  If it is,
			// then add to the Cache LDT.  If it is not, then create a new 
			// User Record in the Segment, and populate the LDT Info (the Site
			// Visit Data) with the LDT data from the DB User Record.
			recordPresent = userRec.updateCache(client, cacheNamespace);
			if (recordPresent) {
				sve.toStorage(client, cacheNamespace, cacheSet, ldtOps);
			} else {
				sve.reloadCache(client, baseNamespace, cacheNamespace, ldtOps);
			}

			String keyStr = userRec.getUserID();

			// At predetermined milestones, perform various actions 
			// Show Simple Status at regular internals.  For the regular large
			// scale tests with 100 threads, we won't hit this very often.
			if( (opNum + threadNumber) % 1000 == 0 ) {
				console.debug("<%s:%s> Thread(%d) Cust#(%d) BaseSet(%s) User#(%d) UserID(%s) Iteration(%d)",
						CLASSNAME, meth, threadNumber, customerSeed, baseSet, 
						userSeed, keyStr, opNum);
			}
			
			// Do a heavy duty scan less frequently.
			if( (opNum + threadNumber) % 2000 == 0 ) {
				Key baseKey = new Key(baseNamespace, baseSet, keyStr);
				Key cacheKey = new Key(cacheNamespace, cacheSet, keyStr);
				
				console.debug("<%s:%s> <<SCAN TEST>> Thread(%d) Cust#(%d) BaseSet(%s) CacheSet(%s) User#(%d) UserID(%s) Iteration(%d)",
						CLASSNAME, meth, threadNumber, customerSeed, baseSet, 
						cacheSet, userSeed, keyStr, opNum);
				

				List<Map<String,Object>> baseResult = null;
				List<Map<String,Object>> cacheResult = null;
				int baseResultSize = 0;
				int baseCheckSize = 0;
				int cacheResultSize = 0;
				int cacheCheckSize = 0;
				
				try {
					baseResult = ldtOps.scanLDT(baseKey);
					cacheResult = ldtOps.scanLDT(cacheKey);
					cacheCheckSize = ldtOps.ldtSize(cacheKey, LDT_BIN);
					if (baseResult != null) {
						baseResultSize = baseResult.size();
						baseCheckSize = ldtOps.ldtSize(baseKey, LDT_BIN);
						if (baseResultSize != baseCheckSize) {
							console.error("<%s:%s> <<BASE SCAN Size Error>> Thread(%d) Cust#(%d) BaseSet(%s) UserID(%s) ScanSide(%d) LDT Size(%d)",
									CLASSNAME, meth, threadNumber, customerSeed, baseSet, 
									keyStr, baseResultSize, baseCheckSize);
						}
					} 
					if (cacheResult != null) {
						cacheResultSize = cacheResult.size();
						cacheCheckSize = ldtOps.ldtSize(cacheKey, LDT_BIN);
						if (cacheResultSize != cacheCheckSize) {
							console.error("<%s:%s> <<CACHE SCAN Size Error>> Thread(%d) Cust#(%d) CacheSet(%s) UserID(%s) ScanSide(%d) LDT Size(%d)",
									CLASSNAME, meth, threadNumber, customerSeed, cacheSet, 
									keyStr, cacheResultSize, cacheCheckSize);
						}
					}
				} catch (AerospikeException ae){
					console.error("Aerospike Error Code(%d) Error Message(%s)",
							ae.getResultCode(), ae.getMessage());
					console.info("Keep on Truckin");
				}
				console.debug("<%s:%s> <<SCAN RESULT>> Thread(%d) Cust#(%d) BaseSet(%s) CacheSet(%s) UserID(%s) BaseLDT(%d) CacheLDT(%d)",
						CLASSNAME, meth, threadNumber, customerSeed, baseSet, 
						cacheSet, keyStr, baseResultSize, cacheResultSize);
			}
		} catch (AerospikeException ae) {
			console.error("Aerospike Error Code(%d) Error Message(%s)",
					ae.getResultCode(), ae.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			console.error("[%s] Problem with Thread(%d) Customer Record: Seed(%d)", 
					"Emulate: doOperation(): ", threadNumber, opNum);
		}	
	}

	/**
	 * For "iteration" number of cycles, generate user-visit events within
	 * the bounds of customer range and user range.
	 */
	public void run() {
		final String meth = "run()";
		ILdtOperations ldtOps = dbOps.getLdtOps();
		
		long startTimeMs = System.currentTimeMillis();
		long endTimeMS = startTimeMs + 1000 * emulationDays * 86400;
		long checkTimeMS = 0;
		long secondStartMS = 0;
		long deltaTimeMS = 0;
		int interTransactionWaitMS = 10;
		long secondCount = 1;
		long opNum = 0;
		
		try {
			// This thread will try to maintain a given TPS load, where it will
			// measure its performance per second and try to speed up or slow
			// down accordingly.
			// In each second, it will attempt to complete "threadTPS" actions
			// by calling the "working function" doOperation().
			console.info("<%s:%s> ThreadNum(%d) Start: ThreadTPS(%d)", 
					CLASSNAME, meth, threadNumber, threadTPS);
			do {
				// We're going to organize ourselves in terms of TPS and
				// second intervals.  For a given number of TPS, we're going
				// to try to fit in that amount of operations, and then sleep
				// for the remaining amount if we're ahead of schedule in each
				// second.  If we fall behind, then we're going to shorten the
				// amount of time between each operation.
				secondStartMS = System.currentTimeMillis();

				for (int i = 0; i < threadTPS; i++) {
					opNum = (secondCount * threadTPS) + i;
					doOperation( (int) opNum, ldtOps );
					Thread.sleep(interTransactionWaitMS);
				} // for each Transaction (per second)
				secondCount++;
				checkTimeMS = System.currentTimeMillis();
				deltaTimeMS = checkTimeMS - secondStartMS;
				console.debug("StartSecond(%d) CheckTime(%d) Delta(%d)", 
						secondStartMS, checkTimeMS, deltaTimeMS);
				if (deltaTimeMS > 1000) {
					if (interTransactionWaitMS > 10) {
						interTransactionWaitMS--;
					} // otherwise, do nothing.
				} else {
					interTransactionWaitMS++;
					Thread.sleep(Math.abs(1000 - deltaTimeMS));
				}
			} while( checkTimeMS < endTimeMS ); // end for each generateCount

		} catch (Exception e) {
			e.printStackTrace();
			console.error("<%s:%s>Problem with Thread(%d) ", 
					CLASSNAME, meth, opNum);
		}

	} // end run()
	
} // end class UserTraffic
