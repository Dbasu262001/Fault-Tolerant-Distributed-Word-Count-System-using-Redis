-- KEYS[1]: name of sorted set (word counts)
-- KEYS[2]: name of stream (input queue)
-- ARGV[1]: group name
-- ARGV[2]: message id
-- Remaining ARGV: word1, count1, word2, count2, ...

-- Basic arg validation
if #KEYS < 2 then
  return redis.error_reply("expected 2 KEYS: sorted_set, stream")
end
if #ARGV < 2 then
  return redis.error_reply("expected at least 2 ARGV: group, msgid")
end
if ((#ARGV - 2) % 2) ~= 0 then
  return redis.error_reply("expected an even number of word/count pairs after the first two ARGV")
end

local zkey = KEYS[1]
local stream = KEYS[2]
local group = ARGV[1]
local msgid = ARGV[2]

-- Apply ZINCRBY for each word/count pair
local updates = 0
for i = 3, #ARGV, 2 do
  local word = ARGV[i]
  local cnt = tonumber(ARGV[i + 1]) or 0
  if cnt ~= 0 then
    redis.call('ZINCRBY', zkey, cnt, word)
    updates = updates + 1
  end
end

-- Acknowledge the processed message
local acked = redis.call('XACK', stream, group, msgid)
redis.call('XDEL', stream, msgid) 

-- Optionally remove the entry from the stream to avoid retaining it
-- (uncomment if you want the message deleted after ack)
-- local removed = redis.call('XDEL', stream, msgid)

-- Return structured info: {acked, updates}
return { acked, updates }