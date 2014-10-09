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

import org.json.simple.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * General Database class for getting, putting, scanning and removing Aerospike
 * records.  This class holds the STATE for interacting with Aerospike.
 */
public class DbOps implements IAppConstants {
	
	// Manage the DB connection and all DB-related info.
	private AerospikeClient client;
	private String host;
	private int port;
	private String namespace;
	ILdtOperations ldtOps;
	String ldtType;
	String ldtBinName = LDT_BIN;

	private WritePolicy writePolicy;
	private Policy policy;

	protected Console console;

	
	/**
	 * Set up vars and connect to AS.
	 */
	protected DbOps(Console console, DbParameters parms, String ldtType) 
			throws AerospikeException 
	{
		
		this.namespace = parms.namespace;
		this.host = parms.host;
		this.port = parms.port;
		this.ldtType = ldtType;
		
		System.out.println("OPEN AEROSPIKE CLIENT");
		this.client = new AerospikeClient(host, port);
		
		// Given the chosen LDT name, pick the appropriate LDT Ops instance.
		// Set up the specific type of LDT we're going to use (LLIST or LMAP).
		try {
			// Create an LDT Ops var for the type of LDT we're using:
			if ("LLIST".equals(ldtType)) {
				this.ldtOps = new LListOperations( client, console );
			} else 	if ("LMAP".equals(ldtType)) {
				this.ldtOps = new LMapOperations( client, console );
			} else {
				console.error("Can't continue without a valid LDT type.");
				return;
			}
		} 	catch (Exception e){
			System.out.println("GENERAL EXCEPTION:" + e);
			e.printStackTrace();
		}
		
		this.writePolicy = new WritePolicy();
		this.writePolicy.timeout = 1000;
		this.writePolicy.maxRetries = 0;
		this.policy = new Policy();
		
		this.console = console;
	}
	
	/**
	 * Scan the entire SET for a customer.  Get back a set of Records that we
	 * can peruse and print.
	 * @param commandObj
	 * @param params
	 */
	public void printSetContents( String set ) {
		console.debug("ENTER ProcessSetQuery: NS("+namespace+") Set("+set+")");

		try {
			ScanSet scanSet = new ScanSet( console );
			scanSet.runScan(client, this.namespace, set);
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.debug("Done with Query");
	} // end processSetQuery()
	
	/**
	 * Scan the user's Site Visit List.  Get back a List of Maps that we
	 * can peruse and print.
	 * @param commandObj
	 * @param params
	 */
	public void printSiteVisitContents( String set, String key ) {
		console.debug("ENTER ProcessSiteQuery");
		
		List<Map<String,Object>> scanList = null;
		
		// We have multiple implementations of this operation:
		// (*) LLIST, with the ordering value on "expire" value.
		// (*) LMAP, with the unique value on "expire" value.	
		scanList = ldtOps.processSiteQuery(this.namespace, set, key);
		
		// Show the results of the site query
		int listSize = 0;
		if (scanList != null) {
			listSize = scanList.size();
		}

		console.debug("Site Visit Entries: Set(%s) UserId(%s) ListCnt(%d)\n", 
				set, key, listSize );

		if (listSize > 0){
			int counter = 1;
			for ( Map<String,Object> mapObject : scanList ) {
				console.debug("(" + counter++ + ") Obj(" + mapObject + ")" );
			}

			console.debug("Site Query Results: " + (counter - 1) + " Objects.");
		}
		
	} // end processSiteQuery()
	
	/**
	 * Remove all records for a given set.   Do a scan, and then for each
	 * record in the scan set, issue a delete.
	 * @param commandObj
	 */
	public void removeSetRecords( String set  ) {
		console.debug("ENTER removeSetRecords");

		List<Key> keyList; // The list of record keys in this set.

		try {
			ScanKeySet scanKeySet = new ScanKeySet( console );
			keyList = scanKeySet.runScan(client, this.namespace, set);
			
			// Process all records (via keys) in the scanSet.
			for (Key key : keyList) {
				console.debug("Key:: " + key );
				client.delete(writePolicy, key);
			}
			
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Scan Delete Exception: " + e);
		}
		console.debug("Done with Query");
	} // end removeSetRecords()
	
	/**
	 * Clean all records for a given set;  Remove all of the expired data
	 * from the LDT bins in every record of the set.
	 * 
	 * Do a scan, and then for each record in the scan set, perform an LDT
	 * clean operation.
	 * 
	 * @param commandObj
	 */
	public void cleanLdtObjectsInSet( String set, ILdtOperations ldtOp  ) {
		console.debug("ENTER cleanLdtObjectsInSet");

		List<Key> keyList; // The list of record keys in this set.

		try {
			ScanKeySet scanKeySet = new ScanKeySet( console );
			keyList = scanKeySet.runScan(client, this.namespace, set);
			
			// Process all records (via keys) in the scanSet.
			for (Key key : keyList) {
				console.debug("Key:: " + key );
				client.delete(writePolicy, key);
			}
			
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Scan LDT Delete Exception: " + e);
		}
		console.debug("Done with Query");
	} // end cleanLdtObjectsInSet()
	
	/**
	 * Remove a specific record, given a set and a keyString.
	 * @param commandObj
	 */
	public void removeRecord( String set, String keyString  ) {
		console.debug("ENTER ProcessRemoveRecord");

		try {
			Key userKey = new Key(this.namespace, set, keyString);
			client.delete( this.writePolicy, userKey );
			
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.debug("Done with Query");
	} // end removeRecord()
	
	
	/**
	 * Remove a specific record, given a set and an Aerospike Key.
	 * 
	 * @param client
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public void  removeRecord(AerospikeClient client, Key key) 
			throws Exception 
	{

		try {
			// Remove the record
			console.debug("Remove Record: key(%s)" + key );
			client.delete(this.writePolicy, key);

		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
	} // end removeRecord()
	
	
	/**
	 * Write an Aerospike record, given a client, a list of bins and a key.
	 */
	public static void writeRecord(AerospikeClient client, Key key, Bin[] binList) 
			throws Exception
	{

	}


	/**
	 * Read and return a fetched Aerospike Record.
	 * @param client
	 * @param key
	 * @param binList
	 * @return
	 * @throws Exception
	 */
	public static Record readRecord(AerospikeClient client, Key key, Bin[] binList) 
			throws Exception 
	{
		Record resultRecord = null;

		return resultRecord;
	}

	public AerospikeClient getClient() {
		return client;
	}

	public void setClient(AerospikeClient client) {
		this.client = client;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public ILdtOperations getLdtOps() {
		return ldtOps;
	}

	public void setLdtOps(ILdtOperations ldtOps) {
		this.ldtOps = ldtOps;
	}
	
	
	
} // end class DbUtil
