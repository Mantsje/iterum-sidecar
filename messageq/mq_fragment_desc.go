package messageq

import (
	"encoding/json"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/Mantsje/iterum-sidecar/transmit"
)

// mqFragmentDesc is a structure describing an iterum fragment to process coming from the MQ
// For now it is a copy of RemoteFragmentDesc but extensible for the future
type MqFragmentDesc struct {
	data.RemoteFragmentDesc
}

func newFragmentDesc(remoteFrag data.RemoteFragmentDesc) MqFragmentDesc {
	fd := MqFragmentDesc{remoteFrag}
	return fd
}

// Serialize tries to transform `mqfd` into a json encoded bytearray. Errors on failure
func (mqfd *MqFragmentDesc) Serialize() (data []byte, err error) {
	data, err = json.Marshal(mqfd)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return
}

// Deserialize tries to decode a json encoded byte array into `mqfd`. Errors on failure
func (mqfd *MqFragmentDesc) Deserialize(data []byte) (err error) {
	err = json.Unmarshal(data, mqfd)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return
}
