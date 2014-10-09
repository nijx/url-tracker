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
local MOD = "CM_LMAP:2014_10_08.A";


-- ======================================================================
-- Must define a table that holds the functions that will be exported
-- and used by the LDT instance.
-- ======================================================================
local userModule = {};

-- ======================================================================
-- Lua Imports
-- ======================================================================
-- Import the functions we will be using in Large Map to perform the
-- LMAP LDT configuration.
local lmap_settings = require('ldt/settings_lmap');

-- General LLIST Functions
local lmap          = require('ldt/lib_lmap');

-- LDT Errors
local ldte           = require('ldt/ldt_errors');

-- ======================================================================
-- adjust_settings()
-- Set this LMAP for best performance using
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

  -- Use a medium amount of Top Record space for the Hash Directory
  lmap_settings.set_hash_dir_size( ldtMap, 256 );

  -- Use a relatively large Compact List (64)
  lmap_settings.set_compact_list_threshold( ldtMap, 64 );

  -- Each Hash Cell can hold up to "threshold" items.  We decide on the
  -- threshold based on object size and hash dir size.
  lmap_settings.set_hash_cell_threshold( ldtMap, 2 );

end -- adjust_settings()

-- ========================================================================
-- expire( topRec, binName, expireVal )
-- ========================================================================
-- Scan the LDT, locate all elements older than the expireVal, then
-- remove them.
-- ========================================================================
function expire( topRec, binName, expireVal )
  local meth = "lmap_expire";
  GP=F and info("[ENTER]<%s:%s>BinNameType(%s) expireVal(%s)",
    MOD, meth, tostring(binName), tostring(expireVal));

  local scanList = lmap.scan(topRec, binName);

  GP=F and info("[DEBUG]<%s:%s> ScanList Shows: %s", MOD, meth, tostring(scanList));

  local expireList = list();
  local objectMap
  for i = 1, #scanList do
    objectMap = scanList[i];
    GP=F and info("[DEBUG]<%s:%s> Examining Object(%s) for expire value", MOD, meth,
      objectMap);
    if objectMap.value.expire < expireVal then
      list.append(expireList, objectMap.name);
    end
  end

  GP=F and info("[DEBUG]<%s:%s> ExpireList Shows: %s", MOD, meth, tostring(expireList));

  for i = i, #expireList do
    lmap.remove(topRec, binName, expireList[i]);
  end

  info("[EXIT]<%s:%s>", MOD, meth );
end -- lmap_expire()


-- ======================================================================
-- Return the value of this module's table so that others importing this
-- module get the table reference.
-- ======================================================================
return userModule;

-- ========================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- ========================================================================
