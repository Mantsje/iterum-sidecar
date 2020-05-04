package config

import (
	"encoding/json"
	"errors"
)

// Config is the struct holding configurable information
// This can be set via the environment variable ITERUM_CONFIG
type Config struct {
	QueueMapping map[string]string `json:"queue_mapping"` // nillable, transformation-step output -> message queue
}

// FromString converts a string value into an instance of Config and also does validation
func (conf *Config) FromString(stringified string) (err error) {
	err = json.Unmarshal([]byte(stringified), &conf)
	if err == nil {
		err = conf.Validate()
	}
	return
}

// Validate does validation of a config struct,
// ensuring that it's members contain valid values
func (conf Config) Validate() error {

	return nil
}

// MapQueue tries to map a transformation step output to a target MQ.
// If either the map is nil or the key does not exist it returns an error
func (conf Config) MapQueue(queue string) (target string, err error) {
	if conf.QueueMapping == nil {
		err = errors.New("QueueMapping is nil")
		return
	}
	target, ok := conf.QueueMapping[queue]
	if !ok {
		err = errors.New("Target queue not in QueueMapping")
	}
	return
}
