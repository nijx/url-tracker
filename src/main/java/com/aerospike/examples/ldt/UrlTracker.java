package com.aerospike.examples.ldt;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

Invocation Parameter Examples:
(*) Invoke this program with "Auto-generate" ...
    ==>      -h 172.16.114.164 -g 50000 -c 10 -r 100
    And, we will create:
     + 10 customer sets
     + 100 User Records in each set
     + 50000 generated Site-Visit operations across the 10 sets and 100 records
       in each set.


@author toby
*/
public class UrlTracker implements IAppConstants {
	
	private String host; // The name of the host node that we'll connect to
	private int port;    // The port of the host node
	private String namespace; // The Namespace that will hold the data
	
	private DbOps dbOps; // Interact with Aerospike Operations
	private DbParameters parms; // The "bundled" object holding DB values.
	
	private AerospikeClient client;// The Aerospike client instance
	private String inputFileName; // for JSON commands
	private String ldtType;		// LDT Type (LLIST or LMAP)
	
	private long customerRecords; // Number of Sets we're going to use
	private long userRecords;     // Number of User Records per set	
	private long generateCount;   // Number of SiteVisit Updates (over all)
	
	private int threadCount;	// Number of threads generating data
	private boolean cleanBefore;// If true, remove all data before test run.
	private boolean cleanAfter; // If true, remove all data after test run.
	
	private boolean noLoad; // If true, just start iterations without a data load.
	private boolean loadOnly; // If true, Load but do NOT perform iterations or clean.

	private int cleanIntervalSec; // Sleep time (in sec) between LDT Clean expiration cycles
	private long cleanDurationSec; // Total amount of time to run the clean threads
	private int cleanMethod; // Do we clean with client code(1) or UDF code(2)?
	
	private long timeToLive; // The minimum length of time for a site obj to live.

	protected Console console; // Easy IO for tracing/debugging

	/**
	 * Constructor for URL Tracker EXAMPLE class.
	 * @param console
	 * @param host
	 * @param port
	 * @param namespace
	 * @param fileName
	 * @param ldtType
	 * @param clean
	 * @param remove
	 * @param customers
	 * @param records
	 * @param generateCount
	 * @param threadCount
	 * @param cleanIntervalSec
	 * @param cleanDurationSec
	 * @param cleanMethod
	 * @param timeToLive
	 * @throws AerospikeException
	 */
	public UrlTracker(Console console, String host, int port, String namespace, 
			String fileName, String ldtType, boolean clean, boolean remove, 
			long customers, long records, long generateCount, int threadCount,
			int cleanIntervalSec, long cleanDurationSec, int cleanMethod,
			long timeToLive,  boolean noLoad, boolean loadOnly
			)  	throws AerospikeException 
	{
		this.host = host;
		this.port = port;
		this.namespace = namespace;
		this.parms = new DbParameters(host, port, namespace);
		
		this.dbOps = new DbOps(console, parms, ldtType);
		
		this.client = dbOps.getClient();
		this.inputFileName = fileName;
		this.ldtType = ldtType;
		this.console = console;
		this.cleanBefore = clean;
		this.cleanAfter = remove;
		this.customerRecords = customers;
		this.threadCount = threadCount;
		this.userRecords = records;
		this.timeToLive = timeToLive;
 
		// If non-zero, then generate data rather than read from JSON file.
		this.generateCount = generateCount; 

		this.cleanIntervalSec = cleanIntervalSec;
		this.cleanDurationSec = cleanDurationSec;
		this.cleanMethod = cleanMethod;
		
		this.noLoad = noLoad;
		this.loadOnly = loadOnly;
	} // end UrlTracker constructor
	
	
	/**
	 * Run the URL Tracker Application.  Based on the input parameters, we
	 * can run with commands read from a JSON input file (the default) or
	 * from a command generator.
	 */
	public void runUrlTracker() {

		try {
			// Our "DbParmeters" object holds all of the Aerospike Server
			// values (host, port, namespace) in a single object.
			DbParameters parms = new DbParameters(host, port, namespace);

			ProcessCommands pc = new ProcessCommands(console, parms, 
					ldtType, dbOps, timeToLive);

			if (generateCount > 0){
				// We are using the command generator to drive this application
				if ( cleanBefore && !noLoad ) {
					pc.cleanDB(customerRecords, userRecords);
				}
				pc.generateCommands(threadCount, customerRecords,
						userRecords, generateCount, cleanIntervalSec, 
						cleanDurationSec, cleanMethod, noLoad, loadOnly );
				if ( cleanAfter && !loadOnly ) {
					pc.cleanDB(customerRecords, userRecords);
				}
			} else {
				// We are using the JSON file to drive this application
				pc.processJSONCommands( this.inputFileName );
			}

		} catch (Exception e) {
			console.error("Critical error::" + e.toString());
		}

	} // end runUrlTracker()
	

	/**
	 * Main Function for URL Tracker.  Get the options from the user and
	 * launch the URL Tracker application.
	 * @param args
	 * @throws AerospikeException
	 */
	public static void main(String[] args) throws AerospikeException {
		Console console = new Console();
		console.info("Starting in Main (1.2.5) \n");

		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");
			options.addOption("d", "debug", false, "Turn on DEBUG level prints.");
			options.addOption("f", "filename", true, "Input File (default: commands.json)");
			options.addOption("t", "type", true, "LDT Type (LMAP or LLIST) (default: LLIST)");
			options.addOption("g", "generate", false, "Generate input data, rather than use Input Command File (default: false");
			options.addOption("c", "customer", true, "Generated Number of Customer Sets (default: 10)");
			options.addOption("r", "records", true, "Generated number of Users per customer (default: 20)");
			options.addOption("v", "visits", true, "Average number of SiteVisits per UserRecord (default: 1000)");
			
			options.addOption("T", "Threads", true, "Number of threads to use in Generate Mode (default: 1)");
			options.addOption("L", "TimeToLive", true, "Number of seconds for a Site Visit Object to live (default: 600)");
			
			options.addOption("I", "CleanInterval", true, "Time to sleep in seconds between cleaning (default: 1200 sec)");
			options.addOption("D", "CleanDuration", true, "Total seconds to run clean threads (default: 3600 sec)");
			options.addOption("M", "CleanMethod", true, "Method for cleaning expired values: 1:client, 2:UDF(default)");
					
			options.addOption("C", "Clean", true, "CLEAN all records at start of run (0==no, 1==yes) (default: 1)");
			options.addOption("R", "Remove", true, "REMOVE all records at END of run (0==no, 1==yes) (default: 1)");
			
			options.addOption("N", "NoLoad", false, "Do NOT load Data: Run ONLY the SiteVisit Updates and Clean");
			options.addOption("O", "LoadOnly", false, "Load the Data, but do NOT update SiteVisits or Clean");

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
			long customers = Long.parseLong(customerString);
			
			String recordString = cl.getOptionValue("r", "20");
			long records = Long.parseLong(recordString);
			
			String visitString = cl.getOptionValue("v", "1000");
			long aveVisits = Long.parseLong(visitString);	
			
			String threadString = cl.getOptionValue("T", "1");
			int threadCount  = Integer.parseInt(threadString);
			
			String cleanString = cl.getOptionValue("C", "1");
			boolean clean  = Integer.parseInt(cleanString) == 1;
			
			// Remove Command: Should we remove data at end of the test?
			String removeString = cl.getOptionValue("R", "1");
			boolean remove  = Integer.parseInt(removeString) == 1;
			
			// Amount to sleep (in seconds) between LDT data cleaning runs
			String intervalString = cl.getOptionValue("I", "1200");
			int intervalSeconds = Integer.parseInt(intervalString);
			
			// Total Duration (in seconds) of the time that we'll let the
			// Cleaning Threads run.
			String durationString = cl.getOptionValue("D", "3600");
			long durationSeconds = Long.parseLong(durationString);
			
			// Time to Live for Site Visit Objects
			String ttlString = cl.getOptionValue("L", "600");
			long timeToLive = Long.parseLong(ttlString);
			
			// Method for Cleaning Expired data:
			// 1: Use the Client Code (scan, get key, call expire method)
			// 2: Use the UDF Code (call UDF Scan to perform expire on the server)
			String cleanMethodString = cl.getOptionValue("M", "2");
			int cleanMethod = Integer.parseInt(cleanMethodString);
			
			@SuppressWarnings("unchecked")
			List<String> cmds = cl.getArgList();
			if (cmds.size() == 0 && cl.hasOption("u")) {
				logUsage(options);
				return;
			}
			
			if (cmds.size() == 0 && cl.hasOption("d")) {
				console.setDebug();
			}
			
			boolean noLoad = false;
			if (cmds.size() == 0 && cl.hasOption("N")) {
				noLoad = true;
			}
			
			boolean loadOnly = false;
			if (cmds.size() == 0 && cl.hasOption("O")) {
				loadOnly = true;
			}
			
			long generateCount = 0L;
			if (cmds.size() == 0 && cl.hasOption("g")) {
				generateCount = aveVisits * customers * records;;
			}

			console.info("Host: " + host);
			console.info("Port: " + port);
			console.info("Namespace: " + namespace);
			console.info("Set: " + set);
			console.info("Input Command FileName: " + fileName);
			console.info("Generate Count: " + generateCount );
			console.info("LDT Type: " + ldtType);
			console.info("Customer Records: " + customers);
			console.info("User Records: " + records);
			console.info("Ave User Site Visit Records: " + aveVisits);
			console.info("Generate: " + generateCount);
			console.info("Threads: " + threadCount);
			console.info("Clean Before: " + clean);
			console.info("Remove After: " + remove);
			console.info("Thread Count: " + threadCount);
			console.info("TimeToLive: " + timeToLive);
			console.info("Clean Interval: " + intervalSeconds);
			console.info("Clean Duration: " + durationSeconds);
			console.info("Clean Method (1 or 2): " + cleanMethod);
			console.info("No Load: " + noLoad);
			console.info("Load Only: " + loadOnly);
	
			
			// Validate the LDT implementation that we're going to use
			if (LLIST.equalsIgnoreCase(ldtType) ||  
				LMAP.equalsIgnoreCase(ldtType) ) 
			{
				console.info("Using LDT Operations:: " + ldtType );
			} else {
				console.error("Unknown LDT Type: " + ldtType);
				console.error("Cannot continue.");
				return;
			}
			
			UrlTracker urlTracker = new UrlTracker(console, host, port, namespace, 
					fileName, ldtType, clean, remove, customers, records, 
					generateCount, threadCount, intervalSeconds, durationSeconds, 
					cleanMethod, timeToLive, noLoad, loadOnly);
			// Run the main application with the given parameters.
			urlTracker.runUrlTracker();

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
		String syntax = UrlTracker.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		System.out.println(sw.toString());
	}

	public DbOps getDbOps() {
		return dbOps;
	}

	public void setDbOps(DbOps dbOps) {
		this.dbOps = dbOps;
	}

	public DbParameters getParms() {
		return parms;
	}

	public void setParms(DbParameters parms) {
		this.parms = parms;
	}

	public String getInputFileName() {
		return inputFileName;
	}

	public void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}

	public long getGenerateCount() {
		return generateCount;
	}

	public void setGenerateCount(long generateCount) {
		this.generateCount = generateCount;
	}

	public int getCleanInterval() {
		return cleanIntervalSec;
	}

	public void setCleanInterval(int cleanInterval) {
		this.cleanIntervalSec = cleanInterval;
	}

	public long getCleanDuration() {
		return cleanDurationSec;
	}

	public void setCleanDuration(long cleanDuration) {
		this.cleanDurationSec = cleanDuration;
	}

	public int getCleanMethod() {
		return cleanMethod;
	}

	public void setCleanMethod(int cleanMethod) {
		this.cleanMethod = cleanMethod;
	}

	public long getTimeToLive() {
		return timeToLive;
	}

	public void setTimeToLive(long timeToLive) {
		this.timeToLive = timeToLive;
	}
	

} // end class UrlTracker