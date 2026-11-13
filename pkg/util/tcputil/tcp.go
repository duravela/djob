package tcputil

import (
	"encoding/binary"
	"net"

	pb "google.golang.org/protobuf/proto"
)

const (
	MaxPoolCapacity       = 64
	ProtoBufferLengthSize = 8
)

func MarshalAndWrite(conn net.Conn, m pb.Message) error {
	p, err := pb.Marshal(m)
	if err != nil {
		conn.Close()
		return err
	}
	return WriteBytesWithLength(conn, p)
}

func WriteBytesWithLength(conn net.Conn, p []byte) error {
	b := make([]byte, ProtoBufferLengthSize)
	binary.LittleEndian.PutUint64(b[0:], uint64(len(p)))
	_, err := conn.Write(b)
	if err != nil {
		return err
	}
	_, err = conn.Write(p)
	return err
}
