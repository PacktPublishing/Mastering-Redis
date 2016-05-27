--[[ Lua script for simple chatroom management ]]--
local chatroom_key = KEYS[1]
for i, email in ipairs(ARGV) do
  redis.call("LPUSH", chatroom_key, email)
end
return redis.call("LLEN", chatroom_key)
