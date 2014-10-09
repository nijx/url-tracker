#URL Tracker Example

## Overview (3)

This application manages customer data, user data and site-visit data.  The purpose of this example
application is to highlight different ways to employ Aerospike Large Data Types (LDTs) to manage
collections of objects -- in this case, the site-visit data.  This example shows two different ways
to manage a collection of "site-visit" values that correspond to user data;  one method uses a Large
Ordered List, and the other method uses a Large Map.

## Introduction

In the Aerospike Database, customer data is kept in a set.   There are many
sets -- one for each customer.  In each customer set, there are user records.
Each user record describes a user.
In each user record, besides the user data, there is a collection of site-visit information that
pertains to a user.
The site-visit data collection is managed by an Aerospike Large Data Type (LDT).

The overall data schema is as follows:

```
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
       |               |Info Data   |        +---------------+||    |    |    |
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

```

In this example application, we show two different ways to manage the site-visit
data.  It can be managed by a Large List, where the ordering criteria is the
expire value;  or, it can be managed by a a Large Map, where the expire value
is the lookup value.

There are five main operations:
1. Add a new Customer Record
2. Add a new user Record
3. For an existing user, add a new Site Visit Object to the collection
4. Query the Site Visit Object (with filters on object fields)
5. Remove all entries that have an expire time < current time.

There are several additional (supportive) operations:
* Query all records in the customer set
* Remove a record (by key)
* Remove all records in a customer set

##Problem

The two main problems addressed by the example application are:

1. How do we represent the Site-Visit Object list?
2. How do we expire site values that are older than a given value?

##Solution

We present two different (although, related) solutions to this problem.

The source code for this solution is available on GitHub, and this README.md 
is in:  `https://github.com/aerospike/url-tracker`

##Installing and Running This Application

This is a Java application that uses Aerospike Java Client (version 3.0.27 or later), eclipse and Maven.
The `pom.xml` file shows Maven how to build the package.
Maven can easily be obtained from: `http://maven.apache.org/`

One set of steps for obtaining, building and running this application would be the following:
1. get the package from github: `git clone git@github.com:aerospike/url-tracker.git`
2. Import the package into eclipse as a git project
3. Set the options (see options below) to point to a running Aerospike server
4. Use the default commands.json file, or create your own.


##The Example Application Invocation

The application is a Java application, built as a Maven Project.  It can be run
inside eclipse (a common choice while developing) or as a stand-alone program using
the generated jar file.  The main class is **UrlTracker** and the main function is `main()`.

In the eclipse tree -- the generated jar file is: `url-tracker-new-1.0.0-full.jar`
Also included in the package is the `runapp` file, which shows how to run the jar file directly:

```
java -cp target/url-tracker-new-1.0.0-full.jar com.aerospike.examples.ldt.UrlTracker
```

The application can be started with the following options:


```
options:
-c,--customer <arg>       Generated Number of Customer Sets (default: 10)
-C,--Clean <arg>          CLEAN all records at start of run (0==no, 1==yes) (default: 1)
-d,--debug                Turn on DEBUG level prints.
-D,--CleanDuration <arg>  Total seconds to run clean threads (default: 3600 sec)
-f,--filename <arg>       Input File (default: commands.json)
-g,--generate             Generate input data, rather than use Input File (default: false)
-h,--host <arg>           Server hostname (default: localhost)
-I,--CleanInterval <arg>  Time to sleep in seconds between cleaning (default: 1200 sec)
-L,--TimeToLive <arg>     Number of seconds for a Site Visit Object to live (default: 600)
-M,--CleanMethod <arg>    Method for cleaning expired values: 1:client, 2:UDF(default)
-n,--namespace <arg>      Namespace (default: test)
-N,--NoLoad               Do NOT load Data: Run ONLY the SiteVisit Updates and Clean
-O,--LoadOnly             Load the Data, but do NOT update SiteVisits or Clean
-p,--port <arg>           Server port (default: 3000)
-r,--records <arg>        Generated number of Users per customer (default: 20)
-R,--Remove <arg>         REMOVE all records at END of run (0==no, 1==yes) (default: 1)
-s,--set <arg>            Set (default: demo)
-t,--type <arg>           LDT Type (LMAP or LLIST) (default: LLIST)
-T,--Threads <arg>        Number of threads to use in Generate Mode (default: 1)
-u,--usage                Print usage.
-v,--visits <arg>         Average number of SiteVisits per UserRecord (default: 1000)
```

By default, the application will expect to connect to an Aerospike Server running
locally (localhost), at port 3000, using namespace "test" and set "demo", reading
data from a local file called "commands.json" and using Large Ordered List (LLIST)
to manage the site-visit collection.

#Program Modes

There are two modes in which the application can: "Generate Mode" and "JSON Data Mode".
"JSON Data Mode" uses a JSON file to feed commands to the URL-Tracker interpreter.
The commands create records, query records and delete records (explained below).
"Generate Mode" creates Customer Records, User Records and Site-Visit records according
to a pattern that is specified by the input parameters.
The "JSON Data Mode" is the default mode, and if there is no override to generate data
(with the -g option), the program will look for a `commands.json` file.  There is an
example file shipped with this package, or a different commands file can be created.

##Data Generate Mode
When the application is driven by a data generator, it uses the following pattern:

* To start in a pristine state, all Customer and User Records are removed
* All Customer Sets and customer records (one record per set) are populated
* For each Customer Set, all User Records are populated
* In a Pseudo-random pattern, Site-Visit records are generated for user records.
* At periodic intervals, a Customer Set will be scanned and EVERY User's record
will be scrubbed of old site-visit records.

Example Invocations, with explanation:

##Invoke the URL-Tracker in generate mode using LLIST 
```
./runapp -h Node_1 -n test -g -c 20 -r 200 -v 40000 -T 8  -M 2 -I 300  -D 84000 -L 600 -t LLIST

```

* -h Node_1 : Connect to a cluster where a node in that cluster is named "Node_1"
* -n test   : Use the namespace named "test"
* -g        : Use Generate Mode (as opposed to the JSON File Command mode)
* -c 20     : Create 20 Customer Sets (and a customer record in each set)
* -r 200    : Create 200 UserRecords in each set
* -v 40000  : Generate 40,000 Site Visit objects in each UserRecord of each Customer Set
* -T 8      : Use 8 parallel threads to write the Site Visit Objects
* -M 2      : Use the UDF mechanism to find and expire the old SiteVisit data objects
* -I 300    : Put the clean threads to sleep for 300 seconds between cleaning phases
* -D 84000  : Run the clean threads for a total duration of 84,000 seconds
* -L 600    : Set the Time To Live for each Site Visit Object to 600 seconds.
* -t LLIST  : Use the LLIST LDT to hold the Site Visit Data in each User Record


##JSON Data Mode
When the application is data driven (data read from the input file), the data file
has the following format:

```
{
    "command_file": "data file name",
    "commands" : [ {"command element"}, {"command element"} ]
}
```

It is just a command_file object with a name and an array of commands.

There are several commands ("command element" instances) that are used for inserting, querying, expiring and removing data:

- Create New Customer ("new_customer")
- Create New User ("new_user")
- Append a new Site-Visit ("new_site_visit")
- Query User ("query_user"): Show all site-visit data for a user.
- Query Customer Set ("query_set"): Show all records in a customer set.
- Remove Expired Site Visit Entries ("remove_expired"): For a given expiration value, remove all site visit data that is older than the expiration value
- Remove Record ("remove_record"): Remove a record based on a record key.
- Remove All Records ("remove_all_records"): Remove all records from a given customer set.


## Data File Example

This file represents the current example commands.json file that is found
in this package.


```
{
    "command_file": "DataFile_1.2",
    "commands": [
    {   "command": "new_customer",
            "customer": {
                "customer_id": "Indigo",
                "contact": "Mr. Raj",
                "set_name": "CustSetOne" 
                }
    },
    {
            "command": "new_user",
            "set_name": "CustSetOne" ,
            "user": {
                "name": "Bob",
                "email": "bob@www.aerospike.com",
                "phone": "(408) 555-1234",
                "address": "1313 Mockingbird Lane",
                "company": "aerospike"
            }
    },
        {
            "command": "new_user",
            "set_name": "CustSetOne" ,
            "user": {
                "name": "Sue",
                "email": "sue@www.aerospike.com",
                "phone": "(408) 555-1234",
                "address": "1313 Mockingbird Lane",
                "company": "aerospike"
            }
    },
        {
            "command": "new_user",
            "set_name": "CustSetOne" ,
            "user": {
                "name": "Joe",
                "email": "joe@www.aerospike.com",
                "phone": "(408) 555-1234",
                "address": "1313 Mockingbird Lane",
                "company": "aerospike"
            }
        },
    {
            "command": "new_user",
            "set_name": "CustSetOne" ,
            "user": {
                "name": "Rick",
                "email": "rick@www.aerospike.com",
                "phone": "(408) 555-1234",
                "address": "1313 Mockingbird Lane",
                "company": "aerospike"
            }
        },
        {
            "command": "new_site_visit",
            "set_name": "CustSetOne" ,
            "user_name": "Bob",
            "visit_info": {
                "url": "www.aerospike.com",
                "referrer": "xyz",
                "page_title": "abc",
                "ip_address": "1.2.3.4",
                "date": 5000,
                "expire": 6000
            }
        },
        {
            "command": "new_site_visit",
            "set_name": "CustSetOne" ,
            "user_name": "Sue",
            "visit_info": {
                "url": "www.aerospike.com",
                "referrer": "xyz",
                "page_title": "abc",
                "ip_address": "1.2.3.4",
                "date": 5001,
                "expire": 6001
            }
        },
        {
            "command": "new_site_visit",
            "set_name": "CustSetOne" ,
            "user_name": "Sue",
            "visit_info": {
                "url": "www.microsoft.com",
                "referrer": "xyz",
                "page_title": "1a2b3c",
                "ip_address": "1.2.3.4",
                "date": 5010,
                "expire": 6010
            }
        },
        {
            "command": "new_site_visit",
            "set_name": "CustSetOne" ,
            "user_name": "Bob",
            "visit_info": {

                "url": "www.google.com",
                "referrer": "xyz",
                "page_title": "abc",
                "ip_address": "1.2.3.4",
                "date": 5020,
                "expire": 6020
            }
        },
        {
            "command": "new_site_visit",
            "set_name": "CustSetOne" ,
            "user_name": "Rick",
            "visit_info": {
                "url": "www.aerospike.com",
                "referrer": "xyz",
                "page_title": "abc",
                "ip_address": "1.2.3.4",
                "date": 5000,
                "expire": 6000
            }
        },
        {
            "command": "new_site_visit",
            "set_name": "CustSetOne" ,
             "user_name": "Bob",
            "visit_info": {
                "url": "www.aerospike.com/documentation",
                "referrer": "xyz",
                "page_title": "abc",
                "ip_address": "1.2.3.4",
                "date": 5030,
                "expire": 6030
            }
        },
        {
            "command": "query_user",
            "set_name": "CustSetOne" ,
            "user_name": "Bob"
        },
        {
            "command": "remove_expired",
            "set_name": "CustSetOne" ,
            "user_name": "Bob",
            "expire": 5500
        },
        {
            "command": "query_set",
            "set_name": "CustSetOne"
        },
    ]
}


```



