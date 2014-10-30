package com.aerospike.examples.ldt;

//import java.io.Console;

/**
 * Application Constants for URL Tracker.  
 * 
 * By "implementing" this interface class, other classes get immediate access
 * to these constants.
 * 
@author toby
*/

public interface IAppConstants {
	
	// LDT Module File Names and LDT Type Names
	public static final String LLIST = "llist";
	public static final String LMAP  = "lmap";
	
	// LDT CreateModule Names
	public static final String CM_LLIST_PATH = "lua/CreateModuleLLIST.lua";
	public static final String CM_LMAP_PATH  = "lua/CreateModuleLMAP.lua";
	
	public static final String CM_LLIST_FILE = "CreateModuleLLIST.lua";
	public static final String CM_LMAP_FILE  = "CreateModuleLMAP.lua";
	
	public static final String CM_LLIST_MOD = "CreateModuleLLIST";
	public static final String CM_LMAP_MOD  = "CreateModuleLMAP";
	
	// LDT expire function names
	public static final String LLIST_EXPIRE = "expire";
	public static final String LMAP_EXPIRE  = "expire";
	public static final String LDT_EXPIRE   = "expire";
	
	// The LDT Bin we'll use for SiteVisit Objects
	public static final String LDT_BIN      = "LDT BIN";
	
	// Cache Record TTL Value (Note that currently we must use the Default TTL
	// that is set on the Namespace -- so this value is not used.
//	public static final int    CACHE_TTL    = 300;  // 5 minutes (for testing)
	public static final int    CACHE_TTL    = 86400; // Expires in one day.
	
	// Aerospike Errors
	public static final int AS_ERR_UNIQUE   = 1402; // Unique Value collision
	
	/**
	 * AppPhases of the test that we'll use for timing measurements.
	 */
	public static enum AppPhases 
		{ START, CLEAN, SETUP, LOAD, UPDATE, SCAN, REMOVE, LAST };

} // end interface IAppConstants