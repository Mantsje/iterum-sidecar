// Package config contains extra custom/optional configuration for the sidecar.
// This is passed as a single stringified JSON object in an environment variable.
// This package deals with parsing it and possibly also downloading relevant
// configuration files that the transformation step may request as defined in
// a pipeline run specification.
package config
