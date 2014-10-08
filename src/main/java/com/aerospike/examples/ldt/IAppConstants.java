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

} // end interface IAppConstants