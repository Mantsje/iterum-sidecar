package messageq

import (
	"encoding/json"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/Mantsje/iterum-sidecar/transmit"
)

// fragmentDesc is a structure describing an iterum fragment to process coming from the MQ
// For now it is a copy of RemoteFragmentDesc but extensible for the future
type fragmentDesc struct {
	data.RemoteFragmentDesc
}

func newFragmentDesc(files []data.RemoteFileDesc) fragmentDesc {
	fd := fragmentDesc{data.RemoteFragmentDesc{}}
	fd.Files = files
	return fd
}

// Serialize tries to transform `mqfd` into a json encoded bytearray. Errors on failure
func (mqfd *fragmentDesc) Serialize() (data []byte, err error) {
	data, err = json.Marshal(mqfd)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return

}

// Deserialize tries to decode a json encoded byte array into `mqfd`. Errors on failure
func (mqfd *fragmentDesc) Deserialize(data []byte) (err error) {
	err = json.Unmarshal(data, mqfd)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return
}
