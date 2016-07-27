-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "coroutine"
local socket = require "socket"
require "string"
require "table"

local async_buffer_size = read_config("async_buffer_size")
local connections = {}
local connections_cnt = 0

local function get_response_code(response)
    local rc = string.match(response, "^HTTP/1.%d (%d%d%d)")
    if rc then return tonumber(rc) end
end

local function send(sequence_id, data, len)
    local c = assert(socket.connect("localhost", 8880))
    c:setoption("keepalive", false)
    local request = string.format("POST /telemetry/id HTTP/1.0\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s", len, data)
    c:send(request)
    local response = ""
    while true do
        c:settimeout(0)   -- do not block
        local buf, err, partial = c:receive(1024)
        if not buf then buf = partial end
        if buf then response = response .. buf end
        if err == "timeout" then
            coroutine.yield(c)
        elseif err == "closed" then
            local rc = get_response_code(response)
            if rc then
                local failure_cnt = 0
                if rc ~= 200 then
                    failure_cnt = 1
                end
                update_checkpoint(sequence_id, failure_cnt)
            end
            break
        end
    end
    c:close()
end

local function scan_connections(idx)
    if idx then
        s = idx
        e = idx
    else
        s = 1
        e = connections_cnt
    end

    for i=s, e do
      local co = connections[i]
      if not co then break end

      local status, res = coroutine.resume(co)
      if not status or not res then
          table.remove(connections, i)
          connections_cnt = connections_cnt - 1
          if not status then error(res) end
      end
    end
end

function process_message(sequence_id)
    scan_connections()
    if connections_cnt >= async_buffer_size then
        return -3, "max connections"
    end

    connections_cnt = connections_cnt + 1
    connections[connections_cnt] = coroutine.create(function () send(sequence_id, "foo", 3) end)
    scan_connections(connections_cnt)
    return -5
end

function timer_event(ns, shutdown)

end

