--[ finish_toy created by Jeremy Nelson for Mastering Redis book 
-- Licensed under GPLv3
--]--
local redis = require 'redis'
local client = redis.connect('127.0.0.1', 6379)
local publisher = redis.connect('127.0.0.1', 6379)
local toys = 0
local channels = {"kanban", "operations" }

function deliver () 
  publisher:publish("kanban", "READY Toy")
  print("Toy delivered")
  toys = toys - 1
end

function build ()
  print("Building Toy "..toys)
  toys = toys + 1
end

function main()
 print("Final Assemble Toy Workstation")
 for msg, abort in client:pubsub({ subscribe = channels}) do
   if msg.kind == 'message' then
     if msg.channel == "operations" then
         if msg.payload == "STOP" then
	   print("Stopping Finish Toy")
           abort()
	 end
     elseif msg.channel == "kanban" then
       if msg.payload == "PULL Toy" then
	 if toys > 0 then
	    deliver()
	    return true
         else
	    print("Pull Paint box")
            publisher:publish("kanban", "PULL Paint")
	 end
       elseif string.find(msg.payload, "READY Painted", 1) then
         build() 
	 deliver()
       end
     end
   end
 end
end
main()
