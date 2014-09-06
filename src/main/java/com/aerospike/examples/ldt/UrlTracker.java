package com.aerospike.examples.ldt;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * UrlTracker application to show Aerospike Data Manipulation for an application
 * that manages customer data, user data and URL Site Visit data.
 * 
 * Overview:
 * 
 * There are many User Records per Customer Set:
    Each User Data Record contains:
    - Key (user id)
    - User Info
    - Site Visit Data List: Each list item is a map, containing:
      (URL, Referrer, Page Title, AgentID, IP Addr, Date, Expire, etc)

               +--------------------------------------------------------------+
               | CUSTOMER SET THREE                                           |
           +-------------------------------------------------------------+    |
           | CUSTOMER SET TWO                                            |    |
       +------------------------------------------------------------+    |    |
       | CUSTOMER SET ONE                                           |    |    |
       |                      Singleton  [ Customer Data ]          |    |    |
       |     User Records                                           |    |    |
       |     +-------------------------------------------------+    |    |    |
       |    +-------------------------------------------------+|    |    |    |
       |   +-------------------------------------------------+||    |    |    |
       |   |     |           |                      |        |||    |    |    |
       |   | Key | User Info | Site Visit Data List | o o o  ||+    |    |    |
       |   |     |           |                      |        |+     |    |    |
       |   +--------------|-----------------------|----------+      |    |    |
       |                  |                       V                 |    |    |
       |                  V                    +---------------+    |    |    |
       |               +------------+         +---------------+|    |    |    |
       |               |Info Object |        +---------------+||    |    |    |
       |               |------------|        |Site Visit Data|||    |    |    |
       |               |* User Name |        |---------------|||    |    |    |
       |               |* Email     |        |* URL          |||    |    |    |
       |               |* Cell phone|        |* Referrer     |||    |    |    |
       |               |* Address   |        |* Page Title   |||    |    |    |
       |               |* Company   |        |* UserAgent-ID |||    |    |    |
       |               |* o o o     |        |* IP Address   |||    |    |----+
       |               |* etc       |        |* Date         ||+    |    |
       |               +------------+        |* Expire       |+     |----+
       |                                     +---------------+      |
       +------------------------------------------------------------+

There are five main operations:
(1) Add a new Customer Record
(2) Add a new user Record
(3) For an existing user, add a new Site Visit Object to the collection
(4) Query the Site Visit Object (with filters on object fields)
(5) Remove all entries that have an expire time < current time.

There are several additional (supportive) operations:
(*) Query all records in the customer set
(*) Remove a record (by key)
(*) Remove all records in a customer set

Invocation Examples:
(*) -h 172.16.114.164 -g 50000 -c 10 -r 100 -v 500

@author toby
*/
public class UrlTracker {
	
	private DbOps dbOps; // Interact with Aerospike.
	
	private AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
	private String set;
	private String inputFileName; // for JSON commands
	private WritePolicy writePolicy;
	private Policy policy;
	private int generateCount;

	protected Console console;

	/**
	 * Constructor for URL Tracker EXAMPLE class.
	 * @param host
	 * @param port
	 * @param namespace
	 * @param set
	 * @param fileName
	 * @throws AerospikeException
	 */
	public UrlTracker(String host, int port, String namespace, String set, 
			String fileName, String ldtType, Console console) 
					throws AerospikeException 
	{
		
		this.dbOps = new DbOps(console, host, port, namespace, ldtType);
		
		this.client = dbOps.getClient();

		this.seedHost = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set; // Set will be overridden by the data.
		this.inputFileName = fileName;
		this.generateCount = 0;  // If non-zero, then generate data rather than
								// read from JSON file.
		
		this.writePolicy = new WritePolicy();
		this.writePolicy.timeout = 1000;
		this.writePolicy.maxRetries = 0;
		this.policy = new Policy();
		this.console = console;
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
		try {
			custRec.toStorage(client, this.namespace);
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
		try {
			userRec.toStorage(client, this.namespace);
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
		
		SiteVisitEntry sve = 
				new SiteVisitEntry(console, commandObj, namespace, 0);
		
		// We have multiple implementations of this operation:
		// (*) LLIST, with the ordering value on "expire" value.
		// (*) LMAP, with the unique value on "expire" value.
		try {
			sve.toStorage(client, namespace, dbOps.getLdtOps());
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
	private void processRemoveExpired(String ns, String set, String key,
			long expire) {
		console.debug("ENTER ProcessRemoveExpired");
		
		// We have multiple implementations of this operation:
		// (*) LLIST, with the ordering value on "expire" value.
		// (*) LMAP, with the unique value on "expire" value.	
		dbOps.getLdtOps().processRemoveExpired(ns, set, key, expire);
	} // processRemoveExpired()

	public static void main(String[] args) throws AerospikeException {
		Console console = new Console();
		console.info("Starting in Main (1.1.7) \n");

		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");
			options.addOption("d", "debug", false, "Turn on DEBUG level prints.");
			options.addOption("f", "filename", true, "Input File (default: commands.json)");
			options.addOption("t", "type", true, "LDT Type (default: LLIST)");
			options.addOption("g", "generate", true, "Generate input data, with N update iterations (default: 0)");
			options.addOption("c", "customer", true, "Generated Number of customer sets (default: 10)");
			options.addOption("r", "records", true, "Generated number of users per customer (default: 20)");
			options.addOption("v", "visits", true, "Generated number of visits per user (default: 500)");
			options.addOption("T", "THREADS", true, "Number of threads to use in Generate Mode (default: 1)");
					
			options.addOption("C", "CLEAN", true, "CLEAN all records at start of run (default 1)");
			options.addOption("R", "REMOVE", true, "REMOVE all records at END of run (default 1)");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);

			String host = cl.getOptionValue("h", "localhost");
			String portString = cl.getOptionValue("p", "3000");
			int port = Integer.parseInt(portString);
			String namespace = cl.getOptionValue("n", "test");
			String set = cl.getOptionValue("s", "demo");
			String fileName = cl.getOptionValue("f", "commands.json");
			String ldtType = cl.getOptionValue("t", "LLIST");
			
			String customerString = cl.getOptionValue("c", "10");
			int customers = Integer.parseInt(customerString);
			
			String recordString = cl.getOptionValue("r", "20");
			int records = Integer.parseInt(recordString);
			
			// Note: Visit Count not currently used.  It is effectively folded
			// into the overall GenerateCount value.
			String visitString = cl.getOptionValue("v", "500");
			int visits = Integer.parseInt(visitString);
		
			String generateString = cl.getOptionValue("g", "0");
			int generateCount = Integer.parseInt(generateString);	
			
			String threadString = cl.getOptionValue("T", "1");
			int threadCount  = Integer.parseInt(threadString);
			
			String cleanString = cl.getOptionValue("C", "1");
			boolean clean  = Integer.parseInt(cleanString) == 1;
			
			String removeString = cl.getOptionValue("R", "1");
			boolean remove  = Integer.parseInt(removeString) == 1;

			console.info("Host: " + host);
			console.info("Port: " + port);
			console.info("Namespace: " + namespace);
			console.info("Set: " + set);
			console.info("FileName: " + fileName);
			console.info("LDT Type: " + ldtType);
			console.info("Customer Records: " + customers);
			console.info("User Records: " + records);
			console.info("User Site Visit Records: " + visits);
			console.info("Generate: " + generateCount );
			console.info("Threads: " + threadCount );
			console.info("Clean Before: " + clean );
			console.info("Remove After: " + remove);
			console.info("Thread Count: " + threadCount );

			@SuppressWarnings("unchecked")
			List<String> cmds = cl.getArgList();
			if (cmds.size() == 0 && cl.hasOption("u")) {
				logUsage(options);
				return;
			}
			
			if (cmds.size() == 0 && cl.hasOption("d")) {
				console.setDebug();
			}
			
			// Validate the LDT implementation that we're going to use
			if ("LLIST".equals(ldtType) ||  "LMAP".equals(ldtType) ) {
				console.info("Using LDT Operations:: " + ldtType );
			} else {
				console.error("Unknown LDT Type: " + ldtType);
				console.error("Cannot continue.");
				return;
			}

			UrlTracker tracker = 
				new UrlTracker(host, port, namespace, set, fileName, ldtType, 
						console );
			if (generateCount > 0){
				if ( clean ) {
					tracker.cleanDB(customers, records);
				}
				tracker.generateCommands( ldtType, generateCount, customers,
						records, visits, threadCount );
				if ( remove ) {
					tracker.cleanDB(customers, records);
				}
			} else {
				tracker.processJSONCommands( ldtType );
			}

		} catch (Exception e) {
			console.error("Critical error::" + e.toString());
		}
	}
	/**
	 * Write usage to console.
	 */
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = LListOperations.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		System.out.println(sw.toString());
	}
	
	/**
	 * cleanDB():  
	 * For the assumed number of customer records and user records, remove them
	 * all from the database.  This function is generally used BEFORE and AFTER
	 * a test run -- to start with a clean DB and end with a clean DB.
	 */
	public void cleanDB( int customers, int userRecords )  {
		
		console.info("Clean DB : Cust(%d) Users(%d) ", customers, userRecords);
		
		// For every Set, Remove every Record.  Don't complain if the records
		// are not there.
		int i = 0;
		UserRecord userRec = null;
		CustomerRecord custRec = null;
		int errorCounter = 0;
		try {
			for (i = 0; i < customers; i++) {
				custRec = new CustomerRecord(console, i);
				custRec.remove(client, namespace);

				for (int j = 0; j < userRecords; j++) {
					userRec = new UserRecord(console, custRec.getCustomerID(), j);
					userRec.remove(client, namespace);
				} // end for each user record	
			} // end for each customer
			
		} catch (Exception e) {
			errorCounter++;
		}

		console.info("End Clean.  ErrorCount(%d)", errorCounter);
	} // end cleanDB()
	
	/**
	 * generateCommands():  Rather than READ the commands from a file, we 
	 * instead GENERATE the commands and then act on them.  We first create
	 * the specified number of Customer Records (and the AS Set for each one),
	 * then we create the specified number of User Records for each Customer.
	 * Then, finally, we use a pseudo-ra the We use a random
	 * distribution of operations
	 * 
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
	public void generateCommands( String ldtType, int generateCount,
			int customers, int userRecords, int visitEntries, int threadCount ) 
	{
		
		console.info("GENERATE COMMANDS: Count(%d) Cust(%d) Users(%d) Visits(%d)", 
				generateCount, customers, userRecords, visitEntries);
		
		ILdtOperations ldtOps = dbOps.getLdtOps();
		
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
		int i = 0;
		Record record = null;
		UserRecord userRec = null;
		CustomerRecord custRec = null;
		SiteVisitEntry sve = null;
		try {
			for (i = 0; i < customers; i++) {
				custRec = new CustomerRecord(console, i);
				custRec.toStorage(client, namespace);

				for (int j = 0; j < userRecords; j++) {
					userRec = new UserRecord(console, custRec.getCustomerID(), j);
					userRec.toStorage(client, namespace);
				} // end for each user record	
			} // end for each customer
		} catch (Exception e) {
			e.printStackTrace();
			console.error("Problem with Customer Record: Seed(%d)", i);
		}
		
		// Start "threadCount" number of threads that will 
		int threadIterations = generateCount / threadCount;
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		console.info("Starting (" + threadCount + ") Threads" );
		for ( int t = 0; t < threadCount; t++ ) {
			console.info("Starting Thread: " + t );
			Runnable userTrafficThread = new UserTraffic(console, client, dbOps,
					namespace, threadIterations, customers, userRecords, t );
			executor.execute( userTrafficThread );
		}
		
		executor.shutdown();
		// Wait until all threads finish.
		while ( !executor.isTerminated() ) {
			// Do nothing
		}
		
		console.info("Finished All Threads");
		
//		
//		try {
//			
//			// Start a steady-State insertion and expiration cycle.
//			// For "Generation Count" iterations, generate a pseudo-random
//			// pattern for a Customer/User record and then insert a site visit
//			// record.  Since we're using TIME (nano-seconds) for the key, we
//			// expect that it will be unique.   If we do get a collision (esp
//			// when we start using multiple threads to drive it), we'll just
//			// retry (which will give us a different nano-time number).
//			//
//			// Also -- we will invoke multiple instances of the client, each
//			// in a thread,  to increase the traffic to the DB cluster (and thus
//			// giving it more exercise).
//			//
//			// New addition:  If our user has specified more than one thread,
//			// then we'll fire off multiple threads
//			Random random = new Random();
//			int customerSeed = 0;
//			int userSeed = 0;
//			String ns = namespace;
//			String set = null;
//			String key = null;
//			Long expire = 0L;
//			console.info("Done with Load.  Starting Site Visit Generation.");
//			for (i = 0; i < generateCount; i++) {
//				customerSeed = random.nextInt(customers);
//				custRec = new CustomerRecord(console, customerSeed);
//				
//				userSeed = random.nextInt(userRecords);
//				userRec = new UserRecord(console, custRec.getCustomerID(), userSeed);
//				
//				sve = new SiteVisitEntry(console, custRec.getCustomerID(), 
//						userRec.getUserID(), i);
//				sve.toStorage(client, namespace, ldtOps);
//				
//				set = custRec.getCustomerID();
//				key = userRec.getUserID();
//				
//				// At predetermined milestones, perform various actions 
//				if( i % 10000 == 0 ) {
//					console.info("Stored Cust#(%d) CustID(%s) User#(%d) UserID(%s) SVE(%d)",
//							customerSeed, set, userSeed, key, i);
//				}
//				if( i % 20000 == 0 ) {
//					console.info("QUERY: Stored Cust#(%d) CustID(%s) User#(%d) UserID(%s) SVE(%d)",
//							customerSeed, set, userSeed, key, i);
//					dbOps.printSiteVisitContents(set, key);
//				}
//				if( i % 30000 == 0 ) {
//					console.info("CLEAN: Stored Cust#(%d) CustID(%s) User#(%d) UserID(%s) SVE(%d)",
//							customerSeed, set, userSeed, key, i);
//					expire = System.nanoTime();
//					processRemoveExpired( ns, set, key, expire );
//				}			
////			} // end for each generateCount
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//			console.error("Problem with Customer Record: Seed(%d)", i);
//		}

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
	public void processJSONCommands( String ldtType ) 
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
			String ns = namespace;
			String set = null;
			String key = null;
			Long expire;

			// Process each value from the JSON array:
			Iterator i = commands.iterator();
			while (i.hasNext()) {
				JSONObject commandObj = (JSONObject) i.next();
				String commandStr = (String) commandObj.get("command");
				System.out.println("Process Command: " + commandStr );

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

} // end class UrlTracker