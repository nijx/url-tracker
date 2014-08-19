
local MOD="2014_08_16.A";

-- ======================================================================
-- LMap Expire Filter
-- ======================================================================
-- This Filter is intended to sift thru LMAP objects and return all items
-- that have an expire number that is past the expiration date passed in.
--
-- Map Objects have the form:
-- (*) expire: Expire Time (in milliseconds) 
-- (*) name: User Name
-- (*) URL: Site Visited
-- (*) referrer: ??
-- (*) page_title: URL Page Title
-- (*) date: Operation Time (in milliseconds)

-- Parms:
-- (1) liveObject: The LMap Object
-- (2) expireValue: the expire number
-- ======================================================================
-- Return:
-- a resultMap of all items that satisfy the filter
-- Nil if nothing satisfies.
-- ======================================================================
function expire_filter( liveObject, expireValue )
  local meth = "expire_filter()";

  GP=F and trace("[ENTER]: <%s:%s> Object(%s) ExpireValue(%s)",
    MOD, meth, tostring(liveObject), tostring(expireValue));

  -- Check the "expireValue" object -=- it must be a valid number.
  if( not expireValue ) then
    warn("[ERROR]<%s:%s> Expire Value is NIL", MOD, meth);
    error("NIL Expire Value");
  end

  if( type( expireValue ) ~= "number" ) then
    warn("[ERROR]<%s:%s> Expire Value is wrong type(%s)",
      MOD, meth, type(expireValue));
    error("BAD Expire Value");
  end

  -- We expect that the "live object" is a map.  Validate that it is
  -- "userdata", specifically of type "Map", and has a field called "expire".
  -- Then, and ONLY then, we'll check the value.
  if( not liveObject ) then
    warn("[ERROR]<%s:%s> DB Object Value is NIL", MOD, meth);
    error("NIL DB Object");
  end

  if( type( liveObject ) ~= "userData" or
     getmetatable( liveObject ) ~= getmetatable( map() )) then
    warn("[ERROR]<%s:%s> Live Object NOT Map Type(%s)",
      MOD, meth, type(liveObject));
    error("BAD DB Object Type");
  end

  local objExpire = liveObject["expire"];
  if not objExpire then
    warn("[ERROR]<%s:%s> DB Object Value Has No Expire Value", MOD, meth);
    error("Object has Empty Expire Field");
  end

  info("[Value Check]<%s:%s> DB Expire(%d) Parm Expire(%d)", MOD, meth,
    objExpire, expireValue );
  if objExpire < expireValue then
    GP=F and trace("[EXIT]<%s:%s> Result(%s)", MOD, meth, tostring(liveObject));
    return liveObject;
  end

  GP=F and trace("[EXIT]<%s:%s> NIL Result", MOD, meth);
  return nil;
end -- range_filter()

<EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --

