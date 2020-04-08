package main

import (
	"encoding/json"

	"github.com/Mantsje/iterum-sidecar/transmit"
)

// FragmentDesc is a structure describing an iterum fragment
// to send to and from the transformation step
type FragmentDesc struct {
	File string `json:"file"`
}

// Serialize tries to transform `f` into a json encoded bytearray. Errors on failure
func (f *FragmentDesc) Serialize() (data []byte, err error) {
	data, err = json.Marshal(f)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return

}

// Deserialize tries to decode a json encoded byte array into `f`. Errors on failure
func (f *FragmentDesc) Deserialize(data []byte) (err error) {
	err = json.Unmarshal(data, f)
	if err != nil {
		err = transmit.ErrSerialization(err)
	}
	return
}
