package env

import (
	"log"
	"os"
	"strings"

	"github.com/iterum-provenance/iterum-go/env"
	"github.com/iterum-provenance/iterum-go/util"
)

const (
	inputSocketEnv  = "TRANSFORMATION_STEP_INPUT"
	outputSocketEnv = "TRANSFORMATION_STEP_OUTPUT"
)

// TransformationStepInputSocket is the path to the socket used for transformation step input
var TransformationStepInputSocket = env.DataVolumePath + "/" + os.Getenv(inputSocketEnv)

// TransformationStepOutputSocket is the path to the socket used for transformation step output
var TransformationStepOutputSocket = env.DataVolumePath + "/" + os.Getenv(outputSocketEnv)

// VerifySidecarEnvs checks whether each of the environment variables returned a non-empty value
func VerifySidecarEnvs() error {
	if !strings.HasSuffix(TransformationStepInputSocket, ".sock") {
		return env.ErrEnvironment(inputSocketEnv, TransformationStepInputSocket)
	} else if !strings.HasSuffix(TransformationStepOutputSocket, ".sock") {
		return env.ErrEnvironment(outputSocketEnv, TransformationStepOutputSocket)
	}
	return nil
}

func init() {
	errIterum := env.VerifyIterumEnvs()
	errMinio := env.VerifyMinioEnvs()
	errMessageq := env.VerifyMessageQueueEnvs()
	errSidecar := VerifySidecarEnvs()

	err := util.ReturnFirstErr(errIterum, errMinio, errMessageq, errSidecar)
	if err != nil {
		log.Fatalln(err)
	}
}
