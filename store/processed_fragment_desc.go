package store

import (
	"encoding/json"

	"github.com/Mantsje/iterum-sidecar/transmit"
)

// ProcessedFragmentDesc is a structure describing an iterum fragment
// to send to and from the transformation step
type ProcessedFragmentDesc struct {
	Files []struct {
		Inputs    []string `json:"input_files"` // Original file name used to create this output file
		LocalPath string   `json:"path"`        // Local path to file
		Name      string   `json:"name"`        // Optional User-specified name
	} `json:"files"`
}

// Serialize tries to transform `pf` into a json encoded bytearray. Errors on failure
func (pf *ProcessedFragmentDesc) Serialize() (data []byte, err error) {
	data, err = json.Marshal(pf)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return

}

// Deserialize tries to decode a json encoded byte array into `pf`. Errors on failure
func (pf *ProcessedFragmentDesc) Deserialize(data []byte) (err error) {
	err = json.Unmarshal(data, pf)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return
}
