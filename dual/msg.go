package dual

import (
	"fmt"
	"github.com/git-disl/GRING"
	"io"
	"encoding/binary"
)

// Ping represents an empty ping message.
type Ping struct{}

// Marshal implements GRING.Serializable and returns a nil byte slice.
func (r Ping) Marshal() []byte { return nil }

// UnmarshalPing returns a Ping instance and never throws an error.
func UnmarshalPing([]byte) (Ping, error) { return Ping{}, nil }

// Pong represents an empty pong message.
type Pong struct{}

// Marshal implements GRING.Serializable and returns a nil byte slice.
func (r Pong) Marshal() []byte { return nil }

// UnmarshalPong returns a Pong instance and never throws an error.
func UnmarshalPong([]byte) (Pong, error) { return Pong{}, nil }

type P2pMessage struct {
      Aggregation uint32
      Opcode byte
      Contents []byte
}

func (m P2pMessage) Marshal() []byte {
      //return []byte(append([]byte{m.Opcode}, m.Contents...))
      var dst []byte
      dst = append(dst, make([]byte, 4)...)
      binary.BigEndian.PutUint32(dst[:4], m.Aggregation)
      dst = append(dst, m.Opcode)
      dst = append(dst, m.Contents...)

      return dst
}

func unmarshalP2pMessage(buf []byte) (P2pMessage, error) {
      //return P2pMessage{Opcode: buf[0], Contents: buf[1:]}, nil
      aggregation := binary.BigEndian.Uint32(buf[:4])
      opcode := buf[4]
      contents := buf[5:]

      return P2pMessage{Aggregation: aggregation, Opcode: opcode, Contents: contents}, nil
}

type DownloadMessage struct {
    UUID []byte //32bytes for plain, 34bytes for encoded
    Contents []byte
}

func (m DownloadMessage) Marshal() []byte {
    var dst []byte
    dst = append(dst, m.UUID...)
    dst = append(dst, m.Contents...)

    return dst
}

func UnmarshalDownloadMessage(buf []byte) (DownloadMessage, error) {
    uuid := buf[0:34]
    contents := buf[35:]

    return DownloadMessage{UUID: uuid, Contents: contents}, nil
}

type Request struct {
    UUID []byte //32bytes for plain, 34bytes for encoded
}

func (m Request) Marshal() []byte {
    var dst []byte
    dst = append(dst, m.UUID...)

    return dst
}

func UnmarshalRequest(buf []byte) (Request, error) {
    uuid := buf[0:34]

    return Request{UUID: uuid}, nil
}

type Deny struct{}

func (r Deny) Marshal() []byte { return nil }

func UnmarshalDeny([]byte) (Deny, error) { return Deny{}, nil }

type GossipMessage struct {
     Opcode byte
     Type byte // type of contents e.g. nested msg type in contents
     UUID []byte //32bytes for plain, 34bytes for encoded
     Count uint32
     TargetID GRING.ID //ID is publickey
     List []byte
     Contents []byte
}

func (m GossipMessage) Marshal() []byte {
     var dst []byte
     dst = append(dst, m.Opcode)
     dst = append(dst, m.Type)
     dst = append(dst, m.UUID...)
     dst = append(dst, make([]byte, 4)...)
     binary.BigEndian.PutUint32(dst[36:40], m.Count)
     dst = append(dst, m.TargetID.Marshal()...)
     if m.Count != 0{
         dst = append(dst, m.List...)
     }
     dst = append(dst, m.Contents...)

     return dst
}

func UnmarshalGossipMessage(buf []byte) (GossipMessage, error) {
    var contents []byte
    var list []byte
    var targetid GRING.ID
    var err error
     opcode := buf[0]
     msgtype := buf[1]
     uuid := buf[2:36]
     count := binary.BigEndian.Uint32(buf[36:40])
     targetid, err = GRING.UnmarshalID(buf[40:])
     if err != nil {
         return GossipMessage{}, io.ErrUnexpectedEOF
     }
     buf = buf[40+targetid.Size():]
     if count == 0{
         contents = buf[0:]
     }else{
         list = buf[0:count]
         contents = buf[count:]
     }

     return GossipMessage{Opcode: opcode, Type: msgtype, UUID: uuid, Count: count, TargetID: targetid, List: list, Contents: contents}, nil
}

type RelayMessage struct {
     UUID []byte //32bytes for plain, 34bytes for encoded. we expect encoded one
     Opcode byte
     Fuel uint32
     TargetID GRING.ID //ID is publickey
     Contents []byte
}

func (m RelayMessage) Marshal() []byte {
     var dst []byte
     dst = append(dst, m.UUID...)
     dst = append(dst, m.Opcode)
     dst = append(dst, make([]byte, 4)...)
     binary.BigEndian.PutUint32(dst[35:39], m.Fuel)
     dst = append(dst, m.TargetID.Marshal()...)
     dst = append(dst, m.Contents...)

     return dst
}

func UnmarshalRelayMessage(buf []byte) (RelayMessage, error) {
     uuid := buf[0:34]
     opcode := buf[34]
     fuel := binary.BigEndian.Uint32(buf[35:39])
     targetid, err := GRING.UnmarshalID(buf[39:])
     if err != nil {
         return RelayMessage{}, io.ErrUnexpectedEOF
     }
     buf = buf[39+targetid.Size():]
     contents := buf[0:]

     return RelayMessage{UUID: uuid, Opcode: opcode, Fuel: fuel, TargetID: targetid, Contents: contents}, nil
}

// FindNodeRequest represents a FIND_NODE RPC call in the Kademlia specification. It contains a target public key to
// which a peer is supposed to respond with a slice of IDs that neighbor the target ID w.r.t. XOR distance.
type FindNodeRequest struct {
	Target GRING.PublicKey
}

// Marshal implements GRING.Serializable and returns the public key of the target for this search request as a
// byte slice.
func (r FindNodeRequest) Marshal() []byte {
	return r.Target[:]
}

// UnmarshalFindNodeRequest decodes buf, which must be the exact size of a public key, into a FindNodeRequest. It
// throws an io.ErrUnexpectedEOF if buf is malformed.
func UnmarshalFindNodeRequest(buf []byte) (FindNodeRequest, error) {
	if len(buf) != GRING.SizePublicKey {
		return FindNodeRequest{}, fmt.Errorf("expected buf to be %d bytes, but got %d bytes: %w",
			GRING.SizePublicKey, len(buf), io.ErrUnexpectedEOF,
		)
	}

	var req FindNodeRequest
	copy(req.Target[:], buf)

	return req, nil
}

// FindNodeResponse returns the results of a FIND_NODE RPC call which comprises of the IDs of peers closest to a
// target public key specified in a FindNodeRequest.
type FindNodeResponse struct {
	Results []GRING.ID
}

// Marshal implements GRING.Serializable and encodes the list of closest peer ID results into a byte representative
// of the length of the list, concatenated with the serialized byte representation of the peer IDs themselves.
func (r FindNodeResponse) Marshal() []byte {
	buf := []byte{byte(len(r.Results))}

	for _, result := range r.Results {
		buf = append(buf, result.Marshal()...)
	}

	return buf
}

// UnmarshalFindNodeResponse decodes buf, which is expected to encode a list of closest peer ID results, into a
// FindNodeResponse. It throws an io.ErrUnexpectedEOF if buf is malformed.
func UnmarshalFindNodeResponse(buf []byte) (FindNodeResponse, error) {
	var res FindNodeResponse

	if len(buf) < 1 {
		return res, io.ErrUnexpectedEOF
	}

	size := buf[0]
	buf = buf[1:]

	results := make([]GRING.ID, 0, size)

	for i := 0; i < cap(results); i++ {
		id, err := GRING.UnmarshalID(buf)
		if err != nil {
			return res, io.ErrUnexpectedEOF
		}

		results = append(results, id)
		buf = buf[id.Size():]
	}

	res.Results = results

	return res, nil
}
