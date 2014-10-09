-- ======================================================================
-- CreateModuleLLIST  (UserModule Example)
-- ======================================================================
-- Copyright [2014] Aerospike, Inc.. Portions may be licensed
-- to Aerospike, Inc. under one or more contributor license agreements.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- ======================================================================

-- Global Print Flags (set "F" to true to print)
local GP;
local F=false;

-- Used for version tracking in logging/debugging
local MOD = "CM_LLIST:2014_10_07.A";


-- ======================================================================
-- Must define a table that holds the functions that will be exported
-- and used by the LDT instance.
-- ======================================================================
local userModule = {};

-- ======================================================================
-- Lua Imports
-- ======================================================================
-- Import the functions we will be using in Large List to perform the
-- LLIST LDT configuration.
local llist_settings = require('ldt/settings_llist');

-- General LLIST Functions
local llist          = require('ldt/lib_llist');

-- LDT Errors
local ldte           = require('ldt/ldt_errors');

-- ======================================================================
-- adjust_settings()
-- Set this LLIST for best performance using
--   + 100byte objects 
--   + 8 byte keys
-- ======================================================================
-- This is a specially named function "adjust_settings()" that the LDT
-- configure code looks for.  If an "adjust_settings()" function exists
-- in this module (it's a Lua Table entry), it will be called, with the
-- ldtMap as a parameter, to initialize this LDT instance on either a
-- create call or on the first insert.
-- ======================================================================
function userModule.adjust_settings( ldtMap )

  -- Use a medium amount of Top Record space for the B+ Tree Root Node
  llist_settings.set_root_list_max( ldtMap, 100 );

  -- With keys + digest == 30bytes, keep the overall sub-rec size under 7kb.
  llist_settings.set_node_list_max( ldtMap, 200 );

  -- With Object Sizes == 100bytes, keep the overall sub-rec size under 7kb.
  llist_settings.set_leaf_list_max( ldtMap, 70 );

  -- Keep no more than 40 100b objects in the Record Compact List.
  llist_settings.set_compact_list_threshold( ldtMap, 40 );

end -- adjust_settings()

-- ========================================================================
-- expire( topRec, binName, expireVal )
-- ========================================================================
-- LLIST FILTER FUNCTION:: for Scan UDF calls.
-- ========================================================================
-- Scan the LDT, locate all elements older than the expireVal, then
-- remove them.
-- ========================================================================
function userModule.expire( topRec, binName, expireVal )
  local meth = "expire";
  GP=F and info("[ENTER]<%s:%s>BinNameType(%s) expireVal(%s)",
    MOD, meth, tostring(binName), tostring(expireVal));

  if ( llist.ldt_exists( topRec, binName, "LLIST" )  == 1 ) then 
    local scanList = llist.scan(topRec, binName);
    GP=F and info("[DEBUG]<%s:%s> ScanList Shows: %s",
      MOD, meth, tostring(scanList));

    local expireList = llist.range(topRec, binName, nil, expireVal);
    GP=F and info("[DEBUG]<%s:%s> ExpireList Shows: %s",
      MOD, meth, tostring(expireList));

    for i = i, #expireList do
      llist.remove(topRec, binName, expireList[i]);
    end
  end -- end if exists

  GP=F and info("[EXIT]<%s:%s>", MOD, meth );
end -- llist_expire()

-- ======================================================================
-- Return the value of this module's table so that others importing this
-- module get the table reference.
-- ======================================================================
return userModule;

-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- ========================================================================
