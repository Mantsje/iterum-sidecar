// Package env contains setup for important environment variables used by the Iterum sidecar.
// Thanks to the init function the usage of these environment variables is checked before they are used
// this ensures that if any of these have an invalid value the applications will crash immediately at the
// start of execution.
package env

import (
	"os"
	"path"
	"strings"

	"github.com/prometheus/common/log"

	"github.com/iterum-provenance/iterum-go/env"
	"github.com/iterum-provenance/iterum-go/process"
	"github.com/iterum-provenance/iterum-go/util"

	"github.com/iterum-provenance/sidecar/env/config"
)

func init() {
	errSidecar := VerifySidecarEnvs()
	errSidecarConf := VerifySidecarConfig()

	err := util.ReturnFirstErr(errSidecar, errSidecarConf)
	if err != nil {
		log.Fatalln(err)
	}
}

const (
	inputSocketEnv  = "TRANSFORMATION_STEP_INPUT"
	outputSocketEnv = "TRANSFORMATION_STEP_OUTPUT"
)

// TransformationStepInputSocket is the path to the socket used for transformation step input
var TransformationStepInputSocket = path.Join(process.DataVolumePath, os.Getenv(inputSocketEnv))

// TransformationStepOutputSocket is the path to the socket used for transformation step output
var TransformationStepOutputSocket = path.Join(process.DataVolumePath, os.Getenv(outputSocketEnv))

// SidecarConfig , if it exists, contains additional configuration information for the sidecar
var SidecarConfig *config.Config = nil

// VerifySidecarEnvs checks whether each of the environment variables returned a non-empty value
func VerifySidecarEnvs() error {
	if !strings.HasSuffix(TransformationStepInputSocket, ".sock") {
		return env.ErrEnvironment(inputSocketEnv, TransformationStepInputSocket)
	} else if !strings.HasSuffix(TransformationStepOutputSocket, ".sock") {
		return env.ErrEnvironment(outputSocketEnv, TransformationStepOutputSocket)
	}
	return nil
}

// VerifySidecarConfig parses and verifies the config struct of the sidecar
func VerifySidecarConfig() error {
	if process.Config != "" {
		c := config.Config{}
		errConfig := c.FromString(process.Config)
		if errConfig != nil {
			return errConfig
		}
		SidecarConfig = &c
	}
	return nil
}
