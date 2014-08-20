#URL Tracker Example

## Overview

This application manages customer data, user data and site-visit data.  The purpose of this example application is to highlight different ways to employ Aerospike Large Data Types (LDTs) to manage collections of objects -- in this case, the site-visit data.  This example shows two different ways to manage a collection of "site-visit"
values that correspond to user data;  one way uses a Large Ordered List, and the other way uses a Large Map.

## Introduction

In the Aerospike Database, customer data is kept in a set.   There are many
sets -- one for each customer.  In each customer set, there are user records.
Each user record describes a user.
In each user record, besides the user data, there is a collection of site-visit information that pertains to a user.
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

1. How do we represent the list?
2. How do we expire site values that are older than a given value?

##Solution

We present two different (although, related) solutions to this problem.

The source code for this solution is available on GitHub, and the README.md 
is in:  `https://github.com/aerospike/url-tracker`

##Installing and Running This Application

This is a Java application that uses Aerospike Java Client (version 3.0.27 or later), eclipse and Maven.  The `pom.xml` file shows Maven how to build the package.
Maven can easily be obtained from: `http://maven.apache.org/`

One set of steps for obtaining, building and running this application would be the following:
1. get the package from github: `git clone git@github.com:aerospike/url-tracker.git`
2. Import the package into eclipse as a git project
3. Set the options (see options below) to point to a running Aerospike server
4. Use the default commands.json file, or create your own.


##The Example Application Invocation

The application is a Java application, built as a Maven Project.  It can be run inside eclipse (a common choice while developing) or as a stand-alone program using the generated jar file.  The main class is **UrlTracker** and the main function is `main()`.

In the eclipse tree -- the generated jar file is: `url-tracker-new-1.0.0-full.jar`

The application can be started with the following options:

* -h  "host"      Name or IP address of AS Node (default:  localhost)
* -p  "port"      Port number of AS Node (default: 3000)
* -n  "namespace  AS Namespace (default: test)
* -s  "set"       AS Set Name (default: demo)
* -u              Print usage options
* -f  "filename", Input FileName (default: commands.json)
* -t "LDT type"   LDT Name for Site-Visit Collection (default: LLIST)
* -g              Generate Data (overrides "-f" option])

By default, the application will expect to connect to an Aerospike Server running locally (localhost), at port 3000, using namespace "test" and set "demo", reading data from a local file called "commands.json" and using Large Ordered List (LLIST) to manage the site-visit collection.

##Application Data
When the application is data driven (data read from the input file), the data file has the following format:

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


```
{
    "command_file": "DataFile_1",
    "commands": [
    {
            "command": "new_user",
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
            "visit_info": {
                "user_name": "Bob",
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
            "visit_info": {
                "user_name": "Sue",
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
            "visit_info": {
                "user_name": "Sue",
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
            "visit_info": {
                "user_name": "Bob",
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
            "visit_info": {
                "user_name": "Rick",
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
            "visit_info": {
                "user_name": "Bob",
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
            "user": "Bob"
    },
    {
            "command": "remove_expired",
            "user": "Bob",
            "expire": 5500
    }
    ]
}

```



