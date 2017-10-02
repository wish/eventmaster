package eventmaster

// Flags is parsed to populate the flags for command eventmaster.
type Flags struct {
	Port int `long:"port" default:"50052" description:"Port for EventMaster gRPC + HTTP API"`

	ConfigFile string `short:"c" long:"config" description:"location of configuration file"`

	RsyslogServer bool `short:"r" long:"rsyslog_server" description:"Flag to start TCP rsyslog server"`
	RsyslogPort   int  `long:"rsyslog_port" default:"50053" description:"Port for rsyslog clients to send logs to"`

	CAFile   string `long:"ca_file" description:"PEM encoded CA's certificate file path"`
	CertFile string `long:"cert_file" description:"PEM encoded certificate file path"`
	KeyFile  string `long:"key_file" description:"PEM encoded private key file path"`

	StaticFiles string `short:"s" long:"static" description:"location of static files to use (instead of embedded files)"`
	Templates   string `short:"t" long:"templates" description:"location of template files to use (instead of embedded)"`
}
