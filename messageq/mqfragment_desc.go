package messageq

import (
	"encoding/json"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/Mantsje/iterum-sidecar/transmit"
)

// MQFragmentDesc is a structure describing an iterum fragment to process coming from the MQ
// For now it is a copy of RemoteFragmentDesc but extensible for the future
type MQFragmentDesc struct {
	*data.RemoteFragmentDesc
}

// Serialize tries to transform `mqm` into a json encoded bytearray. Errors on failure
func (mqm *MQFragmentDesc) Serialize() (data []byte, err error) {
	data, err = json.Marshal(mqm)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return

}

// Deserialize tries to decode a json encoded byte array into `mqm`. Errors on failure
func (mqm *MQFragmentDesc) Deserialize(data []byte) (err error) {
	err = json.Unmarshal(data, mqm)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return
}
