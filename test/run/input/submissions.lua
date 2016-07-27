-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "kafka"
require "string"

local failures = {
    {Type = "test.input", Fields = {
        headers = {"Content-Type", "application/json", "content-length", "3"},
        uri = "/notfound", content = "foo", rv = 404}},
    {Type = "test.input", Fields = {
       headers = {"Content-Type", "application/json", "content-length", "3"},
        uri = "/telemetry/id", content = "foo", method = "GET", rv = 405}},
    {Type = "test.input", Fields = {
       headers = {"Content-Type", "application/json"},
        uri = "/telemetry/id", content = "foo", rv = 411}},
    {Type = "test.input", Fields = {
        headers = {"Content-Type", "application/json", "content-length", "102401"},
        uri = "/telemetry/id", content = string.rep("x", 1024 * 100 + 1), rv = 413}},
    {Type = "test.input", Fields = {headers = {"Content-Type", "application/json", "content-length", "3"},
        uri = "/telemetry/fails/due/to/url/length", content = "foo", rv = 414}},
}


local large_msg_size = 1024 * 64
local large_msg = string.rep("x", large_msg_size)

local tests = {
    {Type = "test.input", Fields = {
        headers = {"Content-Type", "text/plain", "content-length", "3", "DNT", "true"},
        uri = "/telemetry/id", content = "foo", args = "bar=widget&x=1", rv = 200}},
    {Type = "test.input", Fields = {
        headers = {"Content-Type", "text/plain", "content-length", tostring(large_msg_size), "DNT", "true"},
        uri = "/telemetry/id", content = large_msg, args = "bar=widget&x=2", rv = 200}},
}

local brokerlist    = "localhost:9092"
local topics        = {"test"}
local consumer_conf = {["group.id"] = "test", ["message.max.bytes"] = read_config("output_limit")}
local consumer = kafka.consumer(brokerlist, topics, consumer_conf, {["auto.offset.reset"] = "smallest"})
local pb, topic, partition, key = consumer:receive() -- connect to the topic before sending the messages
local hsr = create_stream_reader("kafka")

function process_message()
    for i, v in ipairs(tests) do
        v.Pid = i
        inject_message(v)
    end

    for i, v in ipairs(failures) do
        v.Pid = i
        inject_message(v)
    end

    for i, v in ipairs(tests) do
        print("receive", i)
        while true do
            pb, topic, partition, key = consumer:receive()
            if pb then
                hsr:decode_message(pb)
                local value = hsr:read_message("Logger")
                assert(value == "moz_ingest", tostring(value))
                value = hsr:read_message("Type")
                assert(value == "test", tostring(value))
                value = hsr:read_message("Fields[uri]")
                assert(value == v.Fields.uri, tostring(value))
                value = hsr:read_message("Fields[args]")
                assert(value == v.Fields.args, tostring(value))
                value = hsr:read_message("Fields[content]")
                assert(value == v.Fields.content, tostring(value))
                value = hsr:read_message("Fields[remote_addr]")
                assert(value == "127.0.0.1", tostring(value))
                value = hsr:read_message("Fields[protocol]")
                assert(value == "HTTP/1.1", tostring(value))
                for i = 1, #v.Fields.headers, 2 do
                    local key = v.Fields.headers[i]
                    if key == "content-length" then key = "Content-Length" end
                    value = hsr:read_message("Fields[" .. key .."]")
                    print("key", key, "value", value)
                    if key == "Content-Type" then
                        assert(not value, tostring(value))
                    else
                        assert(value == v.Fields.headers[i + 1], string.format("header: %s key: %s value: %s", v.Fields.headers[i + 1], key, tostring(value)))
                    end
                end
                break
            end
        end
    end

    local cnt = 100000
    for i=1, cnt do
        inject_message({Type = "test.load"})
    end

    for i=1, cnt do
        while true do
            pb, topic, partition, key = consumer:receive()
            if pb then
                hsr:decode_message(pb)
                local value = hsr:read_message("Logger")
                assert(value == "moz_ingest", value)
                value = hsr:read_message("Type")
                assert(value == "test", value)
                break
            end
        end
    end
    return 0, tostring(cnt)
end

