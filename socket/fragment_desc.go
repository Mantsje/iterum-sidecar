package socket

import (
	"encoding/json"

	"github.com/iterum-provenance/sidecar/data"
	"github.com/iterum-provenance/sidecar/transmit"
)

// fragmentDesc is a structure describing a local iterum fragment that
// can be exchanged between the sidecar and the TS
type fragmentDesc struct {
	data.LocalFragmentDesc
}

// newFragmentDesc makes a new fragment description suited for use within socket package
func newFragmentDesc(files []data.LocalFileDesc) fragmentDesc {
	sfd := fragmentDesc{data.LocalFragmentDesc{}}
	sfd.Files = files
	return sfd
}

// Serialize tries to transform `sfd` into a json encoded bytearray. Errors on failure
func (sfd *fragmentDesc) Serialize() (data []byte, err error) {
	data, err = json.Marshal(sfd)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return
}

// Deserialize tries to decode a json encoded byte array into `sfd`. Errors on failure
func (sfd *fragmentDesc) Deserialize(data []byte) (err error) {
	err = json.Unmarshal(data, sfd)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return
}
