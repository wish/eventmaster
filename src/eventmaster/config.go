package main

// Configuration struct for the mongo proxy
// TODO: we want to also allow for a config file, so the "required" check will need to be moved
// out into a method that can do that-- since we'll be loading yaml and CLI args together
type Config struct {
	Port int `long:"port" default:"50052" description:"Port for EventMaster gRPC"` // What port for the API to listen on

	StatsdServer string `long:"statsdserver" description:"server to send statsd stats to"`

	EventStoreName string `long:"eventstorename" required:"true" description:"event store name to talk to"`
}
