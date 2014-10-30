package com.aerospike.examples.ldt;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;

/**
 * Perform Operations on Database Records.
 * 
@author toby
*/

public interface IDbRecord {
	
	
	/**
	 * Take this record and write it to the database.
	 */
	public abstract int toStorage(AerospikeClient client, String namespace) 
			throws Exception;
	
	/**
	 * Read this record from the database.
	 */
	public abstract Record  fromStorage(AerospikeClient client, String namespace) 
			throws Exception;

	/**
	 * Remove this record from the database.
	 */
	public Record  remove(AerospikeClient client, String namespace, String setName) 
			throws Exception;


} // end interface IDbRecord