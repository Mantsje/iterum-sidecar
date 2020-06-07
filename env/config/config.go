package config

import (
	"encoding/json"
	"errors"
	"regexp"

	"github.com/prometheus/common/log"
)

// Config is the struct holding configurable information
// This can be set via the environment variable ITERUM_CONFIG
type Config struct {
	QueueMapping    map[string]string // nillable, transformation-step output -> message queue
	ConfigSelectors []*regexp.Regexp
}

// FromString converts a string value into an instance of Config and also does validation
func (conf *Config) FromString(stringified string) (err error) {
	var placeholder struct {
		QueueMapping map[string]string `json:"queue_mapping"`
		ConfigFiles  []string          `json:"config_files"`
	}
	err = json.Unmarshal([]byte(stringified), &placeholder)
	if err != nil {
		return err
	}
	conf.QueueMapping = placeholder.QueueMapping
	// Parse ConfigSelectors as regexps
	conf.ConfigSelectors = []*regexp.Regexp{}
	for _, selector := range placeholder.ConfigFiles {
		regex, err := regexp.Compile(selector)
		if err != nil {
			return err
		}
		conf.ConfigSelectors = append(conf.ConfigSelectors, regex)
	}
	err = conf.Validate()
	return err
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

// ReturnMatchingFiles returns the list of files matching the ConfigSelectors of conf
func (conf Config) ReturnMatchingFiles(files []string) (matches []string) {
	matches = []string{}
	fullRegexp := ""
	first := true
	// Create 1 big regex
	for _, regex := range conf.ConfigSelectors {
		if first {
			fullRegexp += "(" + regex.String() + ")"
		} else {
			fullRegexp += "| (" + regex.String() + ")"
		}
	}
	if fullRegexp == "" {
		return
	}
	// Compile the big regex
	regex, err := regexp.Compile(fullRegexp)
	if err != nil {
		log.Errorln(err)
		return
	}
	// Match each file against the large regexp once
	for _, file := range files {
		if regex.MatchString(file) {
			matches = append(matches, file)
		}
	}
	return matches
}
