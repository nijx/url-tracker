#URL Tracker Example

This example shows two different ways to manage a collection of "site-visit"
values that correspond to user data.

## Overview

In the Aerospike Database, customer data is kept in a set.   There are many
sets -- one for each customer.  In each customer set, there are user records.
Each user record describes a user.
In each user record, there is a collection of site-visit information that
pertains to a user.  The site-visit data collection is managed by an Aerospike
Large Data Type (LDT).

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
(*) Query all records in the customer set
(*) Remove a record (by key)
(*) Remove all records in a customer set


##Problem

The two main problems addressed by the example application are:

1. How do we represent the list?
2. How do we expire site values that are older than a given value?

##Solution

We present two different (although, related) solutions to this problem.

The source code for this solution is available on GitHub, and the README.md 
http://github.com/some place. 


##Discussion
