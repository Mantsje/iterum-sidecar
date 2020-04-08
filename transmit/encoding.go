package transmit

import (
	"encoding/binary"
	"net"
)

// EncodeSend encodes a serializable object via the Iterum defaults:
//     unsigned 32bit int msg length , followed by the encoded object
// Then it sends it on the target connection
func EncodeSend(conn net.Conn, obj Serializable) (err error) {
	// Encoding
	data, err := obj.Serialize()
	if err != nil {
		return
	}

	size := make([]byte, FragmentSizeLength)
	binary.LittleEndian.PutUint32(size, uint32(len(data)))
	data = append(size, data...)

	// Sending
	_, err = conn.Write(data)
	if err != nil {
		return ErrConnection(err)
	}

	return
}

// DecodeRead tries to decode a serialized object that was encoded
// via the Iterum defaults as described in `transmit.Encode`
// and Read from the passed connection
func DecodeRead(conn net.Conn, obj Serializable) (err error) {
	// Reading
	encMsgSize := make([]byte, FragmentSizeLength)
	_, err = conn.Read(encMsgSize)
	if err != nil {
		return ErrConnection(err)
	}
	msgSize := int(binary.LittleEndian.Uint32(encMsgSize))

	encMsg := make([]byte, msgSize)
	_, err = conn.Read(encMsg)

	// Decoding
	if err != nil {
		return ErrConnection(err)
	}
	err = obj.Deserialize(encMsg)
	return
}
