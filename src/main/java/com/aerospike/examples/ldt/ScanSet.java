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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;

/**
 * @author toby
 *
 */
public class ScanSet implements ScanCallback {

	private int recordCount = 0;
	Console console;
	ArrayList<Record> recordList;

	public ScanSet(Console console) {
		this.console = console;
		this.recordList = new ArrayList<Record>();
	}

	/**
	 * Scan all nodes in parallel and read all records in a List.
	 * @param client
	 * @param namespace
	 * @param set
	 */
	public List<Record> runScan(AerospikeClient client, String namespace, String set) 
			throws Exception 
	{
		console.debug("Scan parallel: namespace=" + namespace + " set=" + set);
		recordCount = 0;
		long begin = System.currentTimeMillis();
		ScanPolicy policy = new ScanPolicy();
		client.scanAll(policy, namespace, set, this);

		long end = System.currentTimeMillis();
		double seconds =  (double)(end - begin) / 1000.0;
		console.debug("Total records returned: " + recordCount);
		console.debug("Elapsed time: " + seconds + " seconds");
		double performance = Math.round((double)recordCount / seconds);
		console.debug("Records/second: " + performance);
		return recordList;
	} // end runScan()


	
	/**
	 * Called from the scan operator for each record.  We use this to accumulate
	 * the contents in a list.
	 * 
	 * @param key : record identifier
	 * @param record : record body
	 */
	public void scanCallback(Key key, Record record) {
		recordCount++;
		
		console.debug("Found Record: Key("+key+") Record(" + record + ")");
		recordList.add(record);

		if ((recordCount % 10000) == 0) {
			console.debug("Records " + recordCount);
		}
	} // end scanCallback()
	
} // end class ScanSet
