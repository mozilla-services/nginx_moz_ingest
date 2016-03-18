-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local http = require("socket.http")
require "ltn12"
require "string"
require "io"

http.TIMEOUT = 2
function process_message()
    local headers = {}
    local method = read_message("Fields[method]") or "POST"
    local uri = "http://localhost:8880" .. read_message("Fields[uri]")
    local args = read_message("Fields[args]")
    if args then
        uri = uri .. "?" .. args
    end
    local hcnt = 0
    while true do
        local key = read_message("Fields[headers]", 0, hcnt)
        if key then
            headers[key] = read_message("Fields[headers]", 0, hcnt  + 1)
            hcnt = hcnt + 2
        else
            break
        end
    end

    local respbody = {}
    local r, c, h = http.request {
        url = uri,
        method = method,
        headers = headers,
        sink = ltn12.sink.table(respbody),
        source = ltn12.source.string(read_message("Fields[content]")),
        }
    assert(c == read_message("Fields[rv]"),  string.format("Test %d rv = %s", read_message("Pid"), tostring(c)))
    return 0
end

function timer_event(ns, shutdown)

end
