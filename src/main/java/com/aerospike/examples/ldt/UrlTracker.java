package com.aerospike.examples.ldt;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

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
 * There are many User Records per Customer Set
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

@author toby
*/
public class UrlTracker {
	private AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
	private String set;
	private String inputFileName;
	private WritePolicy writePolicy;
	private Policy policy;
	private int generateData;

	protected Console console;
	
	// Whichever type LDT we're using, we'll drive with this var.
	protected ILdtOperations ldtOps;

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
			String fileName, Console console) throws AerospikeException 
	{
		System.out.println("OPEN AEROSPIKE CLIENT");
		this.client = new AerospikeClient(host, port);

		this.seedHost = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set; // Set will be overridden by the data.
		this.inputFileName = fileName;
		this.generateData = 0;  // If non-zero, then generate data rather than
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
		
		// We have multiple implementations of this operation:
		// (*) LLIST, with the ordering value on "expire" value.
		// (*) LMAP, with the unique value on "expire" value.		
		ldtOps.processNewSiteVisit(commandObj);
	} // end processNewSiteVisit()


	/**
	 * Scan the user's Site Visit List.  Get back a List of Maps that we
	 * can peruse and print.
	 * @param commandObj
	 * @param params
	 */
	private void processSiteQuery( JSONObject commandObj  ) {
		System.out.println("ENTER ProcessSiteQuery");
		
		// We have multiple implementations of this operation:
		// (*) LLIST, with the ordering value on "expire" value.
		// (*) LMAP, with the unique value on "expire" value.	
		ldtOps.processSiteQuery(commandObj);
	} // end processSiteQuery()
	
	/**
	 * Scan the entire SET for a customer.  Get back a set of Records that we
	 * can peruse and print.
	 * @param commandObj
	 * @param params
	 */
	private void processSetQuery( JSONObject commandObj  ) {
		console.debug("ENTER ProcessSetQuery");

		String custID = (String) commandObj.get("set_name");
		try {
			ScanSet scanSet = new ScanSet( console );
			scanSet.runScan(client, this.namespace, custID);
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.debug("Done with Query");
	} // end processSetQuery()
	
	/**
	 * Remove a specific record.
	 * @param commandObj
	 */
	private void processRemoveRecord( JSONObject commandObj  ) {
		console.debug("ENTER ProcessRemoveRecord");
		
		String userID = (String) commandObj.get("user");
		String custID = (String) commandObj.get("set_name");

		try {
			Key userKey = new Key(this.namespace, custID, userID);
			client.delete( this.writePolicy, userKey );
			
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.debug("Done with Query");
	} // end processRemoveRecord()
	
	/**
	 * Remove all records for a given set.   Do a scan, and then for each
	 * record in the scan set, issue a delete.
	 * @param commandObj
	 */
	private void processRemoveAllRecords( JSONObject commandObj  ) {
		console.debug("ENTER processRemoveAllRecords");

		String custID = (String) commandObj.get("set_name");
		List<Record> recordList;

		try {
			ScanSet scanSet = new ScanSet( console );
			recordList = scanSet.runScan(client, this.namespace, custID);
			
			// NOTE: These operations will be activated shortly.
			// showRecordList( recordList );	
			// deleteRecordList( recordList );
			
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.debug("Done with Query");
	} // end processRemoveAllRecords()
	
	

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
	private void processRemoveExpired( JSONObject commandObj  ) {
		System.out.println("ENTER ProcessRemoveExpired");
		
		// We have multiple implementations of this operation:
		// (*) LLIST, with the ordering value on "expire" value.
		// (*) LMAP, with the unique value on "expire" value.	
		ldtOps.processRemoveExpired(commandObj);
	} // processRemoveExpired()

	public static void main(String[] args) throws AerospikeException {
		Console console = new Console();
		console.info("Starting in Main (1.1.6) \n");

		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");
			options.addOption("f", "filename", true, "Input File (default: commands.json)");
			options.addOption("t", "type", true, "LDT Type (default: LLIST)");
			options.addOption("g", "generate", true, "Generate input data (default: false)");
			options.addOption("c", "customer", true, "Number of customer records (default: 10)");
			options.addOption("r", "records", true, "Ave number of users per customer (default: 20)");
			options.addOption("v", "visits", true, "Ave number of visits per user (default: 500)");

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
			
			String visitString = cl.getOptionValue("v", "500");
			int visits = Integer.parseInt(visitString);
		
			String generateString = cl.getOptionValue("g", "0");
			int generateCount = Integer.parseInt(generateString);

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

			@SuppressWarnings("unchecked")
			List<String> cmds = cl.getArgList();
			if (cmds.size() == 0 && cl.hasOption("u")) {
				logUsage(options);
				return;
			}
			
			// Validate the LDT implementation that we're going to use
			if ("LLIST".equals(ldtType) ||  "LMAP".equals(ldtType) ) {
				console.info("Using LDT Operations:: " + ldtType );
			} else {
				console.error("Unknown LDT Type: " + ldtType);
				console.error("Cannot continue.");
				return;
			}

			UrlTracker tracker = new UrlTracker(host, port, namespace, set, fileName, console );
			if (generateCount > 0){
				tracker.generateCommands( ldtType, generateCount, customers,
						records, visits );
			} else {
				tracker.processCommands( ldtType );
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
	 * generateCommands():  Rather than READ the commands from a file, we 
	 * instead GENERATE the commands and then act on them.  We use a random
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
			int customers, int userRecords, int visitEntries ) 
	{
		
		console.info("GENERATE COMMANDS: Count(%d) Cust(%d) Users(%d) Visits(%d)", 
				generateCount, customers, userRecords, visitEntries);
		
		// Set up the specific type of LDT we're going to use (LLIST or LMAP).
		try {
			// Create an LDT Ops var for the type of LDT we're using:
			if ("LLIST".equals(ldtType)) {
				this.ldtOps = new LListOperations( client, namespace, set, console );
			} else 	if ("LMAP".equals(ldtType)) {
				this.ldtOps = new LMapOperations( client, namespace, set, console );
			} else {
				console.error("Can't continue without a valid LDT type.");
				return;
			}
		} 	catch (Exception e){
			System.out.println("GENERAL EXCEPTION:" + e);
			e.printStackTrace();
		}
		
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

//					for (int k = 0; k < visitEntries; k++) {
//						sve = new SiteVisitEntry(console, 
//								custRec.getCustomerID(), userRec.getUserID(), k);
//						sve.toStorage(client, ldtOps);
//					} // end for each site visit
				} // end for each user record	
			} // end for each customer
			
//			// When debugging -- read everything back.
//			for (i = 0; i < customers; i++) {
//				custRec = new CustomerRecord(console, i);
//				record = custRec.fromStorage(client, namespace);
//				console.debug("Found Customer Record:" + record.toString());
//
//				for (int j = 0; j < userRecords; j++) {
//					userRec = new UserRecord(console, custRec.getCustomerID(), j);
//					record =  userRec.fromStorage(client, namespace);
//					console.debug("Found User Record:" + record.toString());
//
////					for (int k = 0; k < visitEntries; k++) {
////						SiteVisitEntry sve = new SiteVisitEntry(console, 
////								cr.getCustomerID(), ur.getUserID(), k);
////						sve.toStorage(client, ldtOps);
////					} // end for each site visit
//				} // end for each user record	
//			} // end for each customer
			
			// Start a steady-State insertion and expiration cycle.
			// For "Generation Count" iterations, generate a pseudo-random
			// pattern for a Customer/User record and then insert a site visit
			// record.  Since we're using TIME (nano-seconds) for the key, we
			// expect that it will be unique.   If we do get a collision (esp
			// when we start using multiple threads to drive it), we'll just
			// retry (which will give us a different nano-time number).
			Random random = new Random();
			int customerSeed = 0;
			int userSeed = 0;
			console.info("Done with Load.  Starting Site Visit Generation.");
			for (i = 0; i < generateCount; i++) {
				customerSeed = random.nextInt(customers);
				custRec = new CustomerRecord(console, customerSeed);
				
				userSeed = random.nextInt(userRecords);
				userRec = new UserRecord(console, custRec.getCustomerID(), userSeed);
				
				sve = new SiteVisitEntry(console, custRec.getCustomerID(), 
						userRec.getUserID(), i);
				sve.toStorage(client, ldtOps);
				
				if( i % 10000 == 0 ) {
					console.info("Stored Cust(%d); User(%d) SVE(%d)", customerSeed, userSeed, i);
				}
					
			} // end for each generateCount
			
		} catch (Exception e) {
			e.printStackTrace();
			console.error("Problem with Customer Record: Seed(%d)", i);
		}

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
	public void processCommands( String ldtType ) 
			throws IOException, AerospikeException, ParseException  
	{
		System.out.println("PROCESS COMMANDS (a.b.c) \n");
		String inputFileName = this.inputFileName;	

		try {	
			// Create an LDT Ops var for the type of LDT we're using:
			if ("LLIST".equals(ldtType)) {
				this.ldtOps = new LListOperations( client, namespace, set, console );
			} else 	if ("LMAP".equals(ldtType)) {
				this.ldtOps = new LMapOperations( client, namespace, set, console );
			} else {
				console.error("Can't continue without a valid LDT type.");
				return;
			}
			
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
			Iterator i = commands.iterator();

			// take each value from the json array separately
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
					processSiteQuery( commandObj );
				} else if (commandStr.equals( "query_set")) {
					processSetQuery( commandObj );
				} else if (commandStr.equals( "remove_expired")) {
					processRemoveExpired( commandObj );
				} else if (commandStr.equals( "remove_record")) {
					processRemoveRecord( commandObj );
				} else if (commandStr.equals( "remove_all_records")) {
					processRemoveAllRecords( commandObj );
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