package env

import (
	"os"
	"strings"

	"github.com/iterum-provenance/iterum-go/env"
	"github.com/iterum-provenance/iterum-go/util"
	"github.com/iterum-provenance/sidecar/env/config"
	"github.com/prometheus/common/log"
)

const (
	inputSocketEnv  = "TRANSFORMATION_STEP_INPUT"
	outputSocketEnv = "TRANSFORMATION_STEP_OUTPUT"
)

// TransformationStepInputSocket is the path to the socket used for transformation step input
var TransformationStepInputSocket = env.DataVolumePath + "/" + os.Getenv(inputSocketEnv)

// TransformationStepOutputSocket is the path to the socket used for transformation step output
var TransformationStepOutputSocket = env.DataVolumePath + "/" + os.Getenv(outputSocketEnv)

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

// VerifySidecarConfig verifies the config struct of the sidecar
func VerifySidecarConfig() error {
	if env.ProcessConfig == "" {
		log.Infoln("Sidecar was initialized without additional config, make sure that this was intended")
	} else {
		c := config.Config{}
		errConfig := c.FromString(env.ProcessConfig)
		if errConfig != nil {
			return errConfig
		}
		SidecarConfig = &c
	}
	return nil
}

func init() {
	errIterum := env.VerifyIterumEnvs()
	errMinio := env.VerifyMinioEnvs()
	errMessageq := env.VerifyMessageQueueEnvs()
	errSidecar := VerifySidecarEnvs()
	errSidecarConf := VerifySidecarConfig()

	err := util.ReturnFirstErr(errIterum, errMinio, errMessageq, errSidecar, errSidecarConf)
	if err != nil {
		log.Fatalln(err)
	}
}
