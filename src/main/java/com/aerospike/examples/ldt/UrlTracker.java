package com.aerospike.examples.ldt;

//import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
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
		console.info("ENTER ProcessNewCustomer");

		JSONObject userObj = (JSONObject) commandObj.get("customer");

		String customerStr = (String) userObj.get("customer_id");
		String contactStr = (String) userObj.get("contact");
		String custID = (String) userObj.get("set_name");

		try {

			// Note that custID is BOTH the name of the Aerospike SET and it
			// is the KEY of the Singleton Record for Customer info.
			Key key        = new Key(this.namespace, custID, custID);
			Bin custBin    = new Bin("custID", custID);
			Bin nameBin    = new Bin("name", customerStr);
			Bin contactBin = new Bin("contact", contactStr);

			console.info("Put: namespace(%s) set(%s) key(%s) custID(%s) name(%s) contact(%s)",
					key.namespace, key.setName, key.userKey, custBin.value, nameBin.value, contactBin.value );

			// Write the Record
			client.put(this.writePolicy, key, custBin, nameBin, contactBin );
			console.info("Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

			// Verify that we wrote the record.  Read it and validate.
			Record record = client.get(this.policy, key);
			if (record == null) {
				throw new Exception(String.format(
						"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
			}

		} catch (Exception e){
			e.printStackTrace();
			console.info("Exception: " + e);
		}
	} // end processNewCustomer()

	/**
	 * Get the user data from the JSON object and create a new user entry
	 * in Aerospike.
	 * 
	 * @param commandObj
	 */
	private void processNewUser( JSONObject commandObj ) {
		console.info("ENTER ProcessNewUser");

		JSONObject userObj = (JSONObject) commandObj.get("user");

		String nameStr = (String) userObj.get("name");
		String emailStr = (String) userObj.get("email");
		String phoneStr = (String) userObj.get("phone");
		String addressStr = (String) userObj.get("address");
		String companyStr = (String) userObj.get("company");
		String custID = (String) userObj.get("set_name");

		try {

			// The Customer ID is the name of the set.
			Key key = new Key(this.namespace, custID, nameStr);
			Bin userBin = new Bin("userID", nameStr);
			Bin emailBin = new Bin("email", emailStr);
			Bin phoneBin = new Bin("phone", phoneStr);
			Bin addrBin = new Bin("address", addressStr);
			Bin compBin = new Bin("company", companyStr);

			console.info("Put: namespace(%s) set(%s) key(%s) userID(%s) email(%s) phone(%s) address(%s) company(%s)",
					key.namespace, key.setName, key.userKey, userBin.value, emailBin.value, 
					phoneBin.value, addrBin.value, compBin.value);

			client.put(this.writePolicy, key, userBin, emailBin, phoneBin, addrBin, compBin );

			console.info("Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

			Record record = client.get(this.policy, key);

			if (record == null) {
				throw new Exception(String.format(
					"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
			}

		} catch (Exception e){
			e.printStackTrace();
			console.error("Exception: " + e);
		}
	} // end processNewUser()


	/**
	 * Enter a new Site Visit object in the collection of site visits for
	 * a particular user.  Order the Site Visit Objects by Expire Time.
	 * @param commandObj
	 * @param params
	 */
	private void processNewSiteVisit( JSONObject commandObj  ) {
		console.info("ENTER ProcessNewSiteVisit:");
		
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
		console.info("ENTER ProcessSetQuery");

		String custID = (String) commandObj.get("set_name");
		try {
			ScanSet scanSet = new ScanSet( console );
			scanSet.runScan(client, this.namespace, custID);
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.info("Done with Query");
	} // end processSetQuery()
	
	/**
	 * Remove a specific record.
	 * @param commandObj
	 */
	private void processRemoveRecord( JSONObject commandObj  ) {
		console.info("ENTER ProcessRemoveRecord");
		
		String userID = (String) commandObj.get("user");
		String custID = (String) commandObj.get("set_name");

		try {
			Key userKey = new Key(this.namespace, custID, userID);
			client.delete( this.writePolicy, userKey );
			
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.info("Done with Query");
	} // end processRemoveRecord()
	
	/**
	 * Remove all records for a given set.   Do a scan, and then for each
	 * record in the scan set, issue a delete.
	 * @param commandObj
	 */
	private void processRemoveAllRecords( JSONObject commandObj  ) {
		console.info("ENTER processRemoveAllRecords");

		String custID = (String) commandObj.get("set_name");
		List<Record> recordList;

		try {
			ScanSet scanSet = new ScanSet( console );
			recordList = scanSet.runScan(client, this.namespace, custID);
			
//			showRecordList( recordList );
			
//			deleteRecordList( recordList );
			
		} catch (Exception e){
			e.printStackTrace();
			console.warn("Exception: " + e);
		}
		console.info("Done with Query");
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
		console.info("Starting in Main (1.1.4) \n");

		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");
			options.addOption("f", "filename", true, "Input File (default: commands.json)");
			options.addOption("t", "type", true, "LDT Type (default: LLIST)");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);

			String host = cl.getOptionValue("h", "localhost");
			String portString = cl.getOptionValue("p", "3000");
			int port = Integer.parseInt(portString);
			String namespace = cl.getOptionValue("n", "test");
			String set = cl.getOptionValue("s", "demo");
			String fileName = cl.getOptionValue("f", "commands.json");
			String ldtType = cl.getOptionValue("t", "LDT Type");

			console.info("Host: " + host);
			console.info("Port: " + port);
			console.info("Namespace: " + namespace);
			console.info("Set: " + set);
			console.info("FileName: " + fileName);
			console.info("LDT Type: " + ldtType);

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
			tracker.processCommands( ldtType );

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
	 * ProcessCommands():  Read the file of JSON commands and process each one
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
				console.info("The " + i + " element of the array: "+commands.get(i));
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