package com.aerospike.examples.ldt;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.examples.ldt.IAppConstants.AppPhases;

/**
 * This module holds the collection of methods that are used to process commands
 * from the JSON input data file.
 *
@author toby
*/
public class ProcessCommands implements IAppConstants {
	
	private DbOps dbOps; // Interact with Aerospike Operations
	private DbParameters dbParms; // Aerospike values bundled in one object.	
	private String ldtType; // The type of LDT that we'll use
	private AerospikeClient client;
	protected Console console; // Easy IO for tracing/debugging
	private long timeToLive;
	private TestTiming testTiming;
	
	// Constants for Customer Emulation Mode.
	/** Expected number of User Generated Transactions per second.  */
	final int USER_TPS = 1500;
//	final int USER_TPS = 400;
	
	/** Expected Clean Cycle, in seconds */
//	final long CLEAN_CYCLE = 86400; // One days worth of seconds
	final long CLEAN_CYCLE = 600; 
	
	private static final String CLASSNAME = "ProcessCommands";

	/**
	 * Constructor for URL Tracker EXAMPLE class.
	 * @param console
	 * @param dbParms
	 * @param ldtType
	 * @param dbOps
	 * @throws AerospikeException
	 */
	public ProcessCommands(Console console, DbParameters parms, String ldtType, 
			DbOps dbOps, long timeToLive, TestTiming testTiming) throws AerospikeException 
	{
		this.dbParms = parms;
		this.dbOps = dbOps;	
		this.ldtType = ldtType;
		this.client = dbOps.getClient();
		this.console = console;
		this.timeToLive = timeToLive;
		this.testTiming = testTiming;
	}
	
	/**
	 * Get the user data from the JSON object and create a new customer entry
	 * in Aerospike.
	 * 
	 * @param commandObj
	 */
	private void processNewCustomer( JSONObject commandObj ) {
		console.debug("ENTER ProcessNewCustomer");

		JSONObject custObj = (JSONObject) commandObj.get("customer");
		CustomerRecord custRec = new CustomerRecord(console, commandObj, 0);
		String ns = this.dbParms.getNamespace();
		try {
			custRec.toStorage(client, ns);
		} catch (Exception e) {
			e.printStackTrace();
			console.warn("Exception: " + e);
		}

	} // end processNewCustomer()

	/**
	 * Get the user data from the JSON object and create a new user entry
	 * in Aerospike.
	 * 
	 * @param commandObj
	 */
	private void processNewUser( JSONObject commandObj ) {
		console.debug("ENTER ProcessNewUser");

		JSONObject userObj = (JSONObject) commandObj.get("user");
		UserRecord userRec = new UserRecord(console, commandObj, 0);
		String ns = this.dbParms.getNamespace();
		try {
			userRec.toStorage(client, ns);
		} catch (Exception e) {
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
	} // end processNewUser()


	/**
	 * Enter a new Site Visit object in the collection of site visits for
	 * a particular user.  Order the Site Visit Objects by Expire Time.
	 * @param commandObj
	 * @param params
	 */
	private void processNewSiteVisit( JSONObject commandObj  ) {
		console.debug("ENTER ProcessNewSiteVisit:");
		
		String ns = this.dbParms.getNamespace();
		SiteVisitEntry sve = 
				new SiteVisitEntry(console, commandObj, ns, 0, LDT_BIN);
		String set = sve.getCustomerBaseSet();
		
		// We have multiple implementations of this operation:
		// (*) LLIST, with the ordering value on "expire" value.
		// (*) LMAP, with the unique value on "expire" value.
		try {
			sve.toStorage(client, ns, set, dbOps.getLdtOps());
		} catch (Exception e) {
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
	} // end processNewSiteVisit()
	

	/**
	 * Use the Range Query capability of LLIST to find all values between
	 * MIN and Expire.  Then use that result list (if any qualify) as the list
	 * of items to REMOVE from the list.
	 * Note that a value of NIL in the range will start searching at the LEAST
	 * (i.e. the leftmost) item.
	 * 
	 * @param commandObj
	 * @param params
	 */
	private void processRemoveExpired(String ns, String set, String keyStr,
			long expire) 
	{
		console.debug("ENTER ProcessRemoveExpired");

		// We have multiple implementations of this operation:
		// (*) LLIST, with the ordering value on "expire" value.
		// (*) LMAP, with the unique value on "expire" value.	
		try {
			Key key = new Key(ns, set, keyStr);
			dbOps.getLdtOps().processRemoveExpired(ns, set, key, expire);
		} catch (Exception e) {
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
	} // processRemoveExpired()
	
	/**
	 * cleanDB():  
	 * For the assumed number of customer records and user records, remove them
	 * all from the database.  This function is generally used BEFORE and AFTER
	 * a test run -- to start with a clean DB and end with a clean DB.
	 */
	public void cleanDB( long customers, long userRecords, int emulation )  {
		
		console.info("Clean DB : Cust(%d) Users(%d) ", customers, userRecords);
		
		// For every Set, Remove every Record.  Don't complain if the records
		// are not there.   If we're doing an emulation, make sure that we
		// also remove ALL Segmented Cache records (which may be far less in
		// number than the Base User Threads).
		int i = 0;
		UserRecord userRec = null;
		CustomerRecord custRec = null;
		int errorCounter = 0;
		String ns = dbParms.getNamespace();
		String baseNs = dbParms.getBaseNamespace();
		String cacheNs = dbParms.getCacheNamespace();
		try {
			for (i = 0; i < customers; i++) {
				custRec = new CustomerRecord(console, i);
				String custBaseSet = custRec.getCustomerBaseSet();
				String custCacheSet = custRec.getCustomerCacheSet();
				
				// There's a customer record ONLY in the Base Set, not in
				// the Segmented cache set.
				custRec.remove(client, baseNs, custBaseSet);

				for (int j = 0; j < userRecords; j++) {
					userRec = new UserRecord(console, dbOps, custBaseSet, j);
					userRec.remove(client, baseNs, custBaseSet);
				} // end for each user record	
				
				if (emulation > 0) {
					try {
						for (int j = 0; j < userRecords; j++) {
							userRec = new UserRecord(console, dbOps, custBaseSet, j);
							userRec.remove(client, cacheNs, custCacheSet);
						} // end for each user record	
						
					} catch (Exception e){
						// ignore all errors;
					}	
				}
			} // end for each customer
			
		} catch (Exception e) {
			errorCounter++;
		}

		console.info("End Clean.  ErrorCount(%d)", errorCounter);
	} // end cleanDB()
	
	
	/**
	 * Set up the Database.  In this case, this is primarily used for 
	 * registering the User Defined Function (UDF) that we will use for
	 * expiring data items in a Large Data Type (LDT).
	 * 
	 * Although the URL-Tracker app will use only one LDT flavor for a test
	 * run instance, we will register both LLIST and LMAP UDFs here.
	 * 
	 * We have the choice of registering files as "UserModules", which is the
	 * Lua Module that is used when we create an LDT, or we can register the
	 * Lua Module as the container for a filter function.  For this example,
	 * since we're going to do some configuration changes on the LDT, we will
	 * use a UserModule for both LLIST and LMAP.
	 * 
	 * @param moduleName
	 * @param functionName
	 */
	public void databaseSetup() {
		
		RegisterTask task;
		console.info("Register the Create Modules");
		try {

			task = client.register(dbParms.policy, 
					CM_LLIST_PATH, CM_LLIST_FILE, Language.LUA);
			task.waitTillComplete();

			task = client.register(dbParms.policy, 
					CM_LMAP_PATH, CM_LMAP_FILE, Language.LUA);
			task.waitTillComplete();

		} catch (Exception e) {
			e.printStackTrace();
			console.error("Problems with registering Create Modules");
		}
		console.info("Done with the Create Modules");
	} // end databaseSetup()
	

	/**
	 * Simulate the actions of a customer that is tracking User Activity.
	 * Each Customer stores data in a separate Set.  Each Customer Set holds
	 * UserRecords, and each User Record holds URL-Site Visit data.
	 * 
	 * There are 100 customer sets, and about 300 million User Records across
	 * all of the 100 sets (on average, 3 million User Records per customer set).
	 * In each User Record is a variable number of URL-Site Visit data objects,
	 * where each object is about 1kb in size.  On average, most URL Visit 
	 * lists have around 1,000 data objects, but some lists grow to as large as
	 * 100,000 data objects.
	 * 
	 * Each Customer Set has a "Segmented Cache", where the last Day's worth of
	 * User Records are cached there.
	 * 
	 * When a User Record is accessed, it is updated in the regular set, and
	 * then if it does not exist in the associated segmented cache, the entire
	 * LDT is scanned and then written into the cache (along with some other
	 * changes).
	 * 
	 * Records in the Segmented Cache are given a TTL of one day (86400 seconds).
	 * Records are generated by the user base at an average rate of 1500 records
	 * per second.  
	 * 
	 * We use the input parameter 
	 * 
	 * @param customerRecords
	 * @param userRecords
	 * @param noLoad
	 * @param emulationDays
	 */
	public void emulateCustomer( int threadCount, long customerRecords, 
			long userRecords, boolean noLoad, int emulationDays,
			boolean noCleanThreads, int cleanMethod)
	{
		final String meth = "emulateCustomer()";
		
		console.info("EMULATE CUSTOMER: Cust(%d) Users(%d) Days(%d)", 
				customerRecords, userRecords, emulationDays);
		
		String baseNamespace = dbParms.baseNamespace;
		String cacheNamespace = dbParms.cacheNamespace;
		ExecutorService executor;
		int t;
		
		boolean waitResult = true;
		
		// Take care of any Database Setup that is needed.  For the advanced
		// functions, we will need to register any User Defined Functions that
		// we will be using.
		testTiming.setStartTime( AppPhases.SETUP);
		databaseSetup();
		testTiming.setEndTime( AppPhases.SETUP);
		
		// At system startup, we will load the initial state of the database
		// (unless "noLoad
		if (! noLoad){
			testTiming.setStartTime( AppPhases.LOAD);
			executor = Executors.newFixedThreadPool((int)customerRecords);
			console.info("Starting (" + customerRecords + ") Threads for Customer Load." );
			for ( t = 0; t < customerRecords; t++ ) {
				console.info("Starting Customer Load Thread: " + t );
				Runnable loadCustomerThread = new LoadCustomer(console, client,
						dbOps, baseNamespace, t, userRecords);
				executor.execute( loadCustomerThread );
			}
	
			console.info("<%s:%s> Done with Load Thread Generation.  Now waiting to finish",
					CLASSNAME, meth);
			executor.shutdown();
			try {
				// We expect that our Customer and UserRecord loads should 
				// finish in a few minutes.  We'll give them ten minutes as a
				// bounding timeout for the threads to complete.
				// It's the SiteVisit (LDT) writes that could last for days.
				waitResult = executor.awaitTermination(10L, TimeUnit.MINUTES );
			} catch (Exception e){
				console.error("<%s:%s> Load Thread Wait: GENERAL EXCEPTION(%s)",
						CLASSNAME, meth, e.toString());
				e.printStackTrace();
			}
			console.info("<%s:%s>Load Threads Terminated:  WaitResult(%b)",
					CLASSNAME, meth, waitResult);
			
			// Wait until all threads finish.
			while ( !executor.isTerminated() ) {
				// Do nothing
			}
			testTiming.setEndTime( AppPhases.LOAD);
			console.info("<%s:%s> End of Load Phase", CLASSNAME, meth);
		} // end Load Phase
		
		// Start "threadCount" number of threads that will generate updates to
		// the User Records (adding URL Site Visits) and will also access the
		// segmented cache -- either populating it or adding to the segment info.
		// For whatever our threadCount is, we're going to generate USER_TPS
		// number of events, so each thread has to generate:
		// (USER_TPS / threadCount) events per second.
		int threadTPS = USER_TPS / threadCount;
		
		testTiming.setStartTime( AppPhases.UPDATE);
		// Start up the thread executor:  Set up the pool of threads to be the
		// Site Visit threads plus the cleaning threads (one per customer).
		executor = Executors.newFixedThreadPool(threadCount + (int)customerRecords);
		console.info("Starting (" + threadCount + ") Threads for SITE DATA." );
		for ( t = 0; t < threadCount; t++ ) {
			console.info("Starting Thread: " + t );
			Runnable userEmulateThread = new EmulateUser(console, client, dbOps,
					dbParms, threadTPS, emulationDays, customerRecords, 
					userRecords, t, this.timeToLive);
			executor.execute( userEmulateThread );
		}
		
		// Now start the Threads that will perform the Cleaning of Expired
		// LDT Data.  We have two different methods to choose from:
		// 1: We can perform a scan on the client, and for each record we get
		//    back we will extract the LDT Data, and for each expired LDT data
		//    item, we will delete that item.  For LLIST we can perform this
		//    more efficiently because we can do a range scan on the timestamp.
		//    For LMAP we have to scan every item.
		// 2: We can perform a scan with an integrated Record UDF that is called
		//    for each record in the set.  The UDF will scan the LDT and remove
		//    any items that have expired.  We expect that this approach will
		//    significantly outperform the client-side approach.
		if (! noCleanThreads ) {
			console.info("<%s:%s> Cleaning Threads Activated", CLASSNAME, meth);
			long cleanDurationSec = CLEAN_CYCLE * emulationDays;
			if (cleanMethod == 1) {
				// Start up the LDT Cleaning Threads that will scour each
				// Customer Set periodically and remove expired LDT items by
				// bringing data to the client and performing client-side ops.
				console.info("Starting (" + customerRecords + ") Client Cleaning Threads" );
				for ( t = 0; t < customerRecords; t++ ) {
					console.info("Starting Cleaning Thread: " + t );
					Runnable cleanClientThread = new CleanLdtDataFromClient(console, client,
							dbOps, baseNamespace, t, (int)CLEAN_CYCLE, cleanDurationSec, t );
					executor.execute( cleanClientThread );
				} 
			} else {
				// Start up the LDT Cleaning Threads that will scour each
				// Customer Set periodically and remove expired LDT items by
				// invoking a server-side UDF that will do all the work remotely.
				console.info("Starting (" + customerRecords + ") UDF Cleaning Threads" );
				for ( t = 0; t < customerRecords; t++ ) {
					console.info("Starting Cleaning Thread: " + t );
					Runnable cleanUdfThread = new CleanLdtDataWithUDF(console, client,
							dbOps, dbParms, t, (int)CLEAN_CYCLE, cleanDurationSec, t );
					executor.execute( cleanUdfThread );
				} 
			}
		}
		
		// Now collect all of the threads and have them terminate before we
		// move on to the next phase.
		executor.shutdown();
		try {
			// In this case, we expect to run for days.  Set the timeout for
			// our projected number of days.
			waitResult = executor.awaitTermination(emulationDays, TimeUnit.DAYS );
		} catch (Exception e){
			console.error("<%s:%s>Load Thread Wait: GENERAL EXCEPTION(%s)",
					CLASSNAME, meth, e.toString());
			e.printStackTrace();
		}
		console.info("<%s:%s>Update/Clean Threads Terminated:  WaitResult: " + waitResult);
		
		// Wait until all threads finish.
		while ( !executor.isTerminated() ) {
			// Do nothing
		}
		
		testTiming.setEndTime( AppPhases.UPDATE);

		console.info("<%s:%s>Done with User Emulation Session", CLASSNAME, meth);
	} // end emulateCustomer()
	
	/**
	 * generateCommands():  Rather than READ the commands from a file, we 
	 * instead GENERATE the commands and then act on them.  We first create
	 * the specified number of Customer Records (and the AS Set for each one),
	 * then we create the specified number of User Records for each Customer.
	 * Then, finally, we use a pseudo-random number generator to load the
	 * URL SiteVisit data objects.  We use a random distribution of operations
	 * across the different customer sets and user records.
	 */
	public void generateCommands(
			int threadCount, long customerRecords,
			long userRecords, long generateCount, int cleanIntervalSec, 
			long cleanDurationSec, int cleanMethod, boolean noLoad, 
			boolean loadOnly, boolean noCleanThreads, boolean doScan)
	{
		final String meth = "generateCommands()";
		
		console.info("<%s:%s> Count(%d) Cust(%d) Users(%d) T Count(%d)", 
				CLASSNAME, meth, generateCount, customerRecords, userRecords, threadCount);
		
		String namespace = dbParms.namespace;
		ExecutorService executor;
		int t;
		
		boolean waitResult = true;
		
		// Take care of any Database Setup that is needed.  For the advanced
		// functions, we will need to register any User Defined Functions that
		// we will be using.
		testTiming.setStartTime( AppPhases.SETUP);
		databaseSetup();
		testTiming.setEndTime( AppPhases.SETUP);
		
		// For a given number of "generateCount" iterations, we're going to 
		// generate a semi-random set of objects that correspond to Customers, 
		// Users and User-Site Visits.  
		// There are some guidelines:
		// In order to make processing easy, we're going to set up the customer
		// records first, then for each customer, we're going to set up some
		// number of user records, then we will loop thru, generating user-site
		// visit records for a user that corresponds to a customer.
		//
		// We're going to create these records in a pattern, so all we have to
		// remember the customer number, and that will generate a customer
		// record and an entire set of User Records.
		//
		// To make the load faster (and to make better use of the multi-core
		// server nodes), we're going to use multiple threads for the load.
		// We then wait until load is completely finished before we start the
		// update phase.
		if (! noLoad){
			testTiming.setStartTime( AppPhases.LOAD);
			executor = Executors.newFixedThreadPool((int)customerRecords);
			console.info("Starting (" + customerRecords + ") Threads for Customer Load." );
			for ( t = 0; t < customerRecords; t++ ) {
				console.info("Starting Customer Load Thread: " + t );
				Runnable loadCustomerThread = new LoadCustomer(console, client,
						dbOps, namespace, t, userRecords);
				executor.execute( loadCustomerThread );
			}
	
			console.info("Done with Load Thread Generation.  Now waiting to finish");
			executor.shutdown();
			try {
				// We expect that our Customer and UserRecord loads should 
				// finish in a few minutes.  We'll give them ten minutes as a
				// bounding timeout for the threads to complete.
				// It's the SiteVisit (LDT) writes that could last for days.
				waitResult = executor.awaitTermination(10L, TimeUnit.MINUTES );
			} catch (Exception e){
				console.error("<%s:%s>Load Thread Wait: GENERAL EXCEPTION(%s)",
						CLASSNAME, meth, e.toString());
				e.printStackTrace();
			}
			console.info("<%s:%s>Load Threads Terminated:  WaitResult(%b)",
					CLASSNAME, meth, waitResult);
			
			// Wait until all threads finish.
			while ( !executor.isTerminated() ) {
				// Do nothing
			}
			testTiming.setEndTime( AppPhases.LOAD);
			console.info("<%s:%s>End of Load Phase", CLASSNAME, meth);
		} // end Load Phase
		
	
		// If "loadOnly" is true, then we will not launch the SiteObject Update
		// threads or the clean threads.  Just return.
		if (loadOnly){
			return;
		}
		
		// Start "threadCount" number of threads that will populate the
		// User Site Visit LDTs.
		long threadIterations;
		if (threadCount >= generateCount) {
			threadIterations = 1; // don't want this to show up as zero.
		} else {
			threadIterations = generateCount / (long) threadCount;
		}
		
		testTiming.setStartTime( AppPhases.UPDATE);
		// Start up the thread executor:  Set up the pool of threads to be the
		// Site Visit threads plus the cleaning threads (one per customer).
		executor = Executors.newFixedThreadPool(threadCount + (int)customerRecords);
		console.info("Starting (" + threadCount + ") Threads for SITE DATA." );
		for ( t = 0; t < threadCount; t++ ) {
			console.info("Starting Thread: " + t );
			Runnable userTrafficThread = new UserTraffic(console, client, dbOps,
					namespace, threadIterations, customerRecords, userRecords, 
					t, this.timeToLive );
			executor.execute( userTrafficThread );
		}
		
		// Now start the Threads that will perform the Cleaning of Expired
		// LDT Data.  We have two different methods to choose from:
		// 1: We can perform a scan on the client, and for each record we get
		//    back we will extract the LDT Data, and for each expired LDT data
		//    item, we will delete that item.  For LLIST we can perform this
		//    more efficiently because we can do a range scan on the timestamp.
		//    For LMAP we have to scan every item.
		// 2: We can perform a scan with an integrated Record UDF that is called
		//    for each record in the set.  The UDF will scan the LDT and remove
		//    any items that have expired.  We expect that this approach will
		//    significantly outperform the client-side approach.
		if (! noCleanThreads ) {
			console.info("<%s:%s> Cleaning Threads Activated", CLASSNAME, meth);
			if (cleanMethod == 1) {
				// Start up the LDT Cleaning Threads that will scour each
				// Customer Set periodically and remove expired LDT items by
				// bringing data to the client and performing client-side ops.
				console.info("Starting (" + customerRecords + ") Client Cleaning Threads" );
				for ( t = 0; t < customerRecords; t++ ) {
					console.info("Starting Cleaning Thread: " + t );
					Runnable cleanClientThread = new CleanLdtDataFromClient(console, client,
							dbOps, namespace, t, cleanIntervalSec, cleanDurationSec, t );
					executor.execute( cleanClientThread );
				} 
			} else {
				// Start up the LDT Cleaning Threads that will scour each
				// Customer Set periodically and remove expired LDT items by
				// invoking a server-side UDF that will do all the work remotely.
				console.info("Starting (" + customerRecords + ") UDF Cleaning Threads" );
				for ( t = 0; t < customerRecords; t++ ) {
					console.info("Starting Cleaning Thread: " + t );
					Runnable cleanUdfThread = new CleanLdtDataWithUDF(console, client,
							dbOps, dbParms, t, cleanIntervalSec, cleanDurationSec, t );
					executor.execute( cleanUdfThread );
				} 
			}
		}
		
		// Now collect all of the threads and have them terminate before we
		// move on to the next phase.
		executor.shutdown();
		try {
			// We expect that our Customer and UserRecord loads should 
			// finish in a few minutes.  We'll give them ten minutes as a
			// bounding timeout for the threads to complete.
			// It's the SiteVisit (LDT) writes that could last for days.
			waitResult = executor.awaitTermination(10L, TimeUnit.MINUTES );
		} catch (Exception e){
			console.error("<%s:%s>Load Thread Wait: GENERAL EXCEPTION(%s)",
					CLASSNAME, meth, e.toString());
			e.printStackTrace();
		}
		console.info("<%s:%s>Update/Clean Threads Terminated: WaitResult(%b)",
				CLASSNAME, meth, waitResult);
		
		// Wait until all threads finish.
		while ( !executor.isTerminated() ) {
			// Do nothing
		}
		
		testTiming.setEndTime( AppPhases.UPDATE);
			
		// If asked, perform a SCAN phase of the ENTIRE database, reading
		// ALL of the LDTs in EVERY record.  We will do some statistics 
		// gathering from all of the  information coming back from the scans,
		// but we will not do anything with it (such as print or store) because
		// that will greatly slow down the test.
		if ( doScan ){
			testTiming.setStartTime( AppPhases.SCAN);
			executor = Executors.newFixedThreadPool((int)customerRecords);
			console.info("Starting (" + customerRecords + ") Threads for Customer Scan." );
			for ( t = 0; t < customerRecords; t++ ) {
				console.debug("Starting Customer Scan Thread: " + t );
				Runnable scanCustomerThread = 
					new ScanCustomer(console, client, dbOps, 
						namespace, t, t);
				executor.execute( scanCustomerThread );
			}

			console.info("Done with Scan Thread Generation.  Now waiting to finish");
			executor.shutdown();
			try {
				// The time needed to scan all of the customer sets depends on
				// the number of sets, the number of records and the size of
				// the LDTs in the records.  We will make a rough estimate
				// of a conservative ceiling for timeout.
				long timeoutCeiling = customerRecords * userRecords;
				console.info("Scan Thread Timeout Ceiling is (%d) seconds", timeoutCeiling);
				waitResult = executor.awaitTermination(timeoutCeiling, TimeUnit.SECONDS );
			} catch (Exception e){
				System.out.println("Load Thread Wait: GENERAL EXCEPTION:" + e);
				e.printStackTrace();
			}
			console.info("Scan Threads Terminated:  WaitResult: " + waitResult);

			// Wait until all threads finish.
			while ( !executor.isTerminated() ) {
				// Do nothing
			}
			testTiming.setEndTime( AppPhases.SCAN);
			console.info("End of Scan Phase");
		} // end Scan Phase

		console.info("ProcessCommands: Done with GENERATED COMMANDS");
	} // end generateCommands()
	

	/**
	 * processCommands():  Read the file of JSON commands and process each one
	 * of the major commands:
	 * (*) NewUser <data>: Add a new User Record to Set N
	 * (*) NewEntry <data>: Add a new Site Visit entry to User Record in Set N
	 * (*) QueryUser <data>: Fetch all of the Site Data for a User in Set N
	 * (*) RemoveExpired: Remove all Site entries that have expired
	 * 
	 * as well as the minor commands:
	 * (-) ScanSet: Show all records in the customer set
	 * (-) RemoveRecord: Remove a record, by key
	 * (-) RemoveAllRecords: Remove all records in a customer set
	 * 
	 * @throws Exception
	 */
	public void processJSONCommands( String inputFileName ) 
			throws IOException, AerospikeException, ParseException  
	{
		console.info("PROCESS COMMANDS :: JSON File(" + inputFileName + ")");

		try {	
			
			// read the json file
			FileReader reader = new FileReader(inputFileName);

			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);

			// get a Command File from the outer  JSON object
			String commandFile = (String) jsonObject.get("command_file");
			console.info("The command name is: " + commandFile);

			// get an array from the JSON object
			JSONArray commands = (JSONArray) jsonObject.get("commands");

			// take the elements of the json array
			for(int i=0; i< commands.size(); i++){
				console.debug("The " + i + " element of the array: "+commands.get(i));
			}
			
			// Vars to reuse in each case.
			String ns = this.dbParms.namespace;
			String set = null;
			String key = null;
			Long expire;

			// Process each value from the JSON array:
			Iterator i = commands.iterator();
			while (i.hasNext()) {
				JSONObject commandObj = (JSONObject) i.next();
				String commandStr = (String) commandObj.get("command");
				console.debug("Process Command: " + commandStr );

				if( commandStr.equals("new_customer") ) {
					processNewCustomer( commandObj );
				} else if( commandStr.equals("new_user") ) {
					processNewUser( commandObj );
				} else if (commandStr.equals( "new_site_visit")) {
					processNewSiteVisit( commandObj );
				} else if (commandStr.equals( "query_user")) {
					set = (String) commandObj.get("set_name");
					key = (String) commandObj.get("user_name");
					dbOps.printSiteVisitContents(set, key );
				} else if (commandStr.equals( "query_set")) {
					set = (String) commandObj.get("set_name");
					dbOps.printSetContents( set );
				} else if (commandStr.equals( "remove_expired")) {
					set = (String) commandObj.get("set_name");
					key = (String) commandObj.get("user_name");
					expire = (Long) commandObj.get("expire");
					processRemoveExpired( ns, set, key, expire );
				} else if (commandStr.equals( "remove_record")) {
					set = (String) commandObj.get("set_name");
					key = (String) commandObj.get("user_name");
					dbOps.removeRecord(set, key );
				} else if (commandStr.equals( "remove_all_records")) {
					set = (String) commandObj.get("set_name");
					dbOps.removeSetRecords(set);
				}
			} // for each command

		} catch (FileNotFoundException ex) {
			System.out.println("FILE NOT FOUND EXCEPTION:" + ex);
			ex.printStackTrace();
		} catch (IOException ex) {
			System.out.println("INPUT/OUTPUT EXCEPTION:" + ex);
			ex.printStackTrace();
		} 
		catch (NullPointerException ex) {
			ex.printStackTrace();
		} /** catch (AerospikeException ae ){
			ae.printStackTrace();
			System.out.println("AEROSPIKE EXCEPTION");
		} **/
		catch (Exception e){
			System.out.println("GENERAL EXCEPTION:" + e);
			e.printStackTrace();
		}

		console.info("ProcessCommands: Done with Input File: " +inputFileName);

	} // end processCommands()

	public DbOps getDbOps() {
		return dbOps;
	}

	public void setDbOps(DbOps dbOps) {
		this.dbOps = dbOps;
	}

	public DbParameters getParms() {
		return dbParms;
	}

	public void setParms(DbParameters parms) {
		this.dbParms = parms;
	}

	public String getLdtType() {
		return ldtType;
	}

	public void setLdtType(String ldtType) {
		this.ldtType = ldtType;
	}
	
	

} // end class ProcessCommands