# fail-mqttd

## About

[fail-mqttd](https://github.com/juur/fail-mqttd) is a terrible implementation of [MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html). Plus, for extra fail, [The Raft Algorithm](https://raft.github.io/) is also half-done.

## Done, or, half-jobbed

There are various things it might do, hopefully:

- Control Packets (except for AUTH)
- QoS 0, 1 or 2
- Will Messages
- Retained Message
- Session Expiry, reconnection and Clean Start
- UTF-8 validation
- Property parsing
- Topic Filters and topic 'hierarchy'
- Non-shared Subscriptions
- A basic Raft algorthim

## TODO

There are various things it doesn't at all:

- *persistence*: only topics and retained messages are stored to disk.
- *availability*: there is a basic implementation (bug ridden) of the Raft algorithm, but it doesn't hook in properly yet.
- *security*: there is no (useful) authentication, authorisation, confidentiality or integrity.
- multi-threaded operation (partial support)
- few properties are implemented
- Shared Subscriptions (partial support)
