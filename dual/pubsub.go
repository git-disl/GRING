package dual

import (
	"strings"
	//"encoding/json"
	"encoding/binary"
        "github.com/google/uuid"
	//"net"
	"github.com/git-disl/GRING"
	"io"
	"sync"
	//"fmt"
	"net"
)

//protocol opcode
const (
    NOOP                        = 0x00
    // Project state
    STATE_PUBLISHED             = 0x01
    STATE_ACTIVE                = 0x02
    STATE_COMMITTED             = 0x03

    MSG_TYPE_ADMSG              = 0x04
    MSG_TYPE_SUBSMSG            = 0x05
)

type PubSub struct {
    MapPidToPjt map[string]*Project
}

// TODO : remove export later
type Project struct {
    Owner GRING.ID // project owner
    PjtID string
    PjtState byte //project state
    SubList map[string]Subscriber
    //SubList map[string]interface{}
    mutex_sub_list *sync.Mutex
}

// internal data. user cannot control
// PubSub protocol creates N tokens every T time
type ADtoken struct {


    PjtID string
    NumToken uint32
    MaxNumToken uint32
}

type UserInfo interface{}

type Subscriber struct {
    Key string // string of node ID
    Addr string //string of IP:Port
    UserInfo UserInfo // user should define the information field
}

type ADMessage struct {
    PjtID []byte
    PubID GRING.ID
    PjtDesc []byte
    //ExpDate []byte // TODO: how to convert this ? location field in Time is a pointer
}

func (m ADMessage) Marshal() []byte {
    var dst []byte
    dst = append(dst, m.PjtID...)
    dst = append(dst, m.PubID.Marshal()...)
    dst = append(dst, m.PjtDesc...)

    return dst
}

func UnmarshalADMessage(buf []byte) (ADMessage, error) {
    pjtid := buf[0:34]
    pubid, err := GRING.UnmarshalID(buf[34:])
    if err != nil {
        return ADMessage{}, io.ErrUnexpectedEOF
    }
    buf = buf[34+pubid.Size():]
    pjtdesc := buf[0:]

    return ADMessage{PjtID: pjtid, PubID: pubid, PjtDesc: pjtdesc}, nil
}

// subscriber replies with this message to join the project
type SubsMessage struct {
    //PjtID GRING.ID //TODO: add pjt ID for multi project
    SubID GRING.ID // ID is publickey
    Host net.IP `json:"address"` //subs ip
    Port uint16 //subs port
    StaticInfo []byte
}

//TODO
func (m SubsMessage) Marshal() []byte {
    var dst []byte
    dst = append(dst, m.SubID.Marshal()...)
    addr := make([]byte, net.IPv4len+2)
    copy(addr[:net.IPv4len], m.Host)
    binary.BigEndian.PutUint16(addr[net.IPv4len:net.IPv4len+2], m.Port)
    dst = append(dst, addr...)
    dst = append(dst, m.StaticInfo...)

    return dst
}

func UnmarshalSubsMessage(buf []byte) (SubsMessage, error) {
    subid, err := GRING.UnmarshalID(buf)
    if err != nil {
        return SubsMessage{}, io.ErrUnexpectedEOF
    }
    buf = buf[subid.Size():]

    if len(buf) < net.IPv4len {
        return SubsMessage{}, io.ErrUnexpectedEOF
    }
    host := make([]byte, net.IPv4len)
    copy(host, buf[:net.IPv4len])
    buf = buf[net.IPv4len:]

    if len(buf) < 2 {
        return SubsMessage{}, io.ErrUnexpectedEOF
    }
    port := binary.BigEndian.Uint16(buf[:2])

    staticinfo := buf[2:]

    return SubsMessage{SubID: subid, Host: host, Port: port, StaticInfo: staticinfo}, nil
}

func CreateUUID() string{
    uuidWithHyphen := uuid.New()
    uuid := strings.Replace(uuidWithHyphen.String(), "-", "", -1)
    return uuid
}
func NewPubSub() *PubSub {
    pubsub := &PubSub{
                  MapPidToPjt: make(map[string]*Project, 0),
    }

    return pubsub
}

func (ps *PubSub) NewProject(owner GRING.ID) (string, *Project) {
    project := &Project{
                   Owner: owner,
		   PjtID: CreateUUID(),
                   PjtState: STATE_PUBLISHED,
                   SubList: make(map[string]Subscriber, 0),
                   mutex_sub_list: new(sync.Mutex),
    }

    ps.MapPidToPjt[project.PjtID] = project

    return project.PjtID, project
}

func (ps *PubSub) GetProject(pjtid string) (*Project,bool) {
    pjt, found := ps.MapPidToPjt[pjtid]
    return pjt, found
}

func (p *Project) Size() int{
    return len(p.SubList)
}

func (p *Project) GetSubList() map[string]Subscriber{
    return p.SubList
}

func (p *Project) GetPjtID() string{
    return p.PjtID
}

func (p *Project) SetPjtState(state byte) {
    if state == STATE_PUBLISHED || state == STATE_ACTIVE || state == STATE_COMMITTED {
        p.PjtState = state
    }
}

func (p *Project) GetPjtState() byte {
    return p.PjtState
}

func (p *Project) AddSubscriber(subID string, subs Subscriber) {
    p.mutex_sub_list.Lock()
    p.SubList[subID] = subs
    p.mutex_sub_list.Unlock()
}

