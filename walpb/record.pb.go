// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: record.proto

package walpb

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"
import _ "github.com/gogo/protobuf/gogoproto"

import io "io"
import github_com_gogo_protobuf_proto "github.com/gogo/protobuf/proto"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

// Operation identifies type of record
type Operation int32

const (
	// CREATE creates key-value pair
	Operation_CREATE Operation = 0
	// UPDATE deletes key-value pair
	Operation_UPDATE Operation = 1
	// PUT creates or updates existing key-value pair
	Operation_PUT Operation = 2
	// DELETE deletes key-value pair
	Operation_DELETE Operation = 3
	// REOPEN is a system record signalling
	// readers/writers to re-open the database (after compaction)
	Operation_REOPEN Operation = 4
)

var Operation_name = map[int32]string{
	0: "CREATE",
	1: "UPDATE",
	2: "PUT",
	3: "DELETE",
	4: "REOPEN",
}
var Operation_value = map[string]int32{
	"CREATE": 0,
	"UPDATE": 1,
	"PUT":    2,
	"DELETE": 3,
	"REOPEN": 4,
}

func (x Operation) Enum() *Operation {
	p := new(Operation)
	*p = x
	return p
}
func (x Operation) String() string {
	return proto.EnumName(Operation_name, int32(x))
}
func (x *Operation) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(Operation_value, data, "Operation")
	if err != nil {
		return err
	}
	*x = Operation(value)
	return nil
}
func (Operation) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_record_6547f2e696acbf8b, []int{0}
}

// Record is a record stored in operation log file
type Record struct {
	// ProcessID identifies different processes writing
	// to the log, processes have to increase monotonically
	// and be different for different processes writing
	// to the same file to preserve integrity
	ProcessID uint64 `protobuf:"varint,1,req,name=ProcessID" json:"ProcessID"`
	// ID is a record ID that is monothonically incremented by each process
	// Several records have the same id if they carry on the value split
	// into multiple containers, ID is monothonically incremented by each process.
	ID uint64 `protobuf:"varint,2,req,name=ID" json:"ID"`
	// PartID is used to identify a part index (starting from 0)
	// in a multi-part record, subsequent parts have incremental
	// ids (0, 1, 2, 3 ...)
	// Together, (ProcessID, ID, PartID) form a unique
	// position index 3-tuple in a log.
	PartID int32 `protobuf:"varint,3,req,name=PartID" json:"PartID"`
	// LastPart is false when record is split into multiple parts
	// and this is not the last part, true if this is a last record
	// of the multi-part record. If PartID is 0 and LastPart is true
	// it means that this is a single part record.
	LastPart bool `protobuf:"varint,4,req,name=LastPart" json:"LastPart"`
	// Operation defines operation type
	Operation Operation `protobuf:"varint,5,req,name=Operation,enum=walpb.Operation" json:"Operation"`
	// Expires is an optional Unix seconds epoch expiration time
	// for this record, if 0 means record does not expire
	Expires int64 `protobuf:"varint,6,opt,name=Expires" json:"Expires"`
	// Key is the key in the key value tuple being created, updated or deleted
	Key []byte `protobuf:"bytes,7,opt,name=Key" json:"Key,omitempty"`
	// Val is set for all operations except DELETE, REOPEN
	Val                  []byte   `protobuf:"bytes,8,opt,name=Val" json:"Val,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Record) Reset()         { *m = Record{} }
func (m *Record) String() string { return proto.CompactTextString(m) }
func (*Record) ProtoMessage()    {}
func (*Record) Descriptor() ([]byte, []int) {
	return fileDescriptor_record_6547f2e696acbf8b, []int{0}
}
func (m *Record) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Record) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Record.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *Record) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Record.Merge(dst, src)
}
func (m *Record) XXX_Size() int {
	return m.Size()
}
func (m *Record) XXX_DiscardUnknown() {
	xxx_messageInfo_Record.DiscardUnknown(m)
}

var xxx_messageInfo_Record proto.InternalMessageInfo

// State represents database state
type State struct {
	// SchemaVersion is a version of the database schema
	SchemaVersion uint64 `protobuf:"varint,1,req,name=SchemaVersion" json:"SchemaVersion"`
	// ProcessID identifies different processes writing
	// to the log, processes have to increase monotonically
	// and be different for different processes writing
	// to the same file to preserve integrity
	ProcessID uint64 `protobuf:"varint,2,req,name=ProcessID" json:"ProcessID"`
	// CurrentFile points to current file name
	CurrentFile          string   `protobuf:"bytes,3,req,name=CurrentFile" json:"CurrentFile"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *State) Reset()         { *m = State{} }
func (m *State) String() string { return proto.CompactTextString(m) }
func (*State) ProtoMessage()    {}
func (*State) Descriptor() ([]byte, []int) {
	return fileDescriptor_record_6547f2e696acbf8b, []int{1}
}
func (m *State) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *State) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_State.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *State) XXX_Merge(src proto.Message) {
	xxx_messageInfo_State.Merge(dst, src)
}
func (m *State) XXX_Size() int {
	return m.Size()
}
func (m *State) XXX_DiscardUnknown() {
	xxx_messageInfo_State.DiscardUnknown(m)
}

var xxx_messageInfo_State proto.InternalMessageInfo

func init() {
	proto.RegisterType((*Record)(nil), "walpb.Record")
	proto.RegisterType((*State)(nil), "walpb.State")
	proto.RegisterEnum("walpb.Operation", Operation_name, Operation_value)
}
func (m *Record) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Record) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x8
	i++
	i = encodeVarintRecord(dAtA, i, uint64(m.ProcessID))
	dAtA[i] = 0x10
	i++
	i = encodeVarintRecord(dAtA, i, uint64(m.ID))
	dAtA[i] = 0x18
	i++
	i = encodeVarintRecord(dAtA, i, uint64(m.PartID))
	dAtA[i] = 0x20
	i++
	if m.LastPart {
		dAtA[i] = 1
	} else {
		dAtA[i] = 0
	}
	i++
	dAtA[i] = 0x28
	i++
	i = encodeVarintRecord(dAtA, i, uint64(m.Operation))
	dAtA[i] = 0x30
	i++
	i = encodeVarintRecord(dAtA, i, uint64(m.Expires))
	if m.Key != nil {
		dAtA[i] = 0x3a
		i++
		i = encodeVarintRecord(dAtA, i, uint64(len(m.Key)))
		i += copy(dAtA[i:], m.Key)
	}
	if m.Val != nil {
		dAtA[i] = 0x42
		i++
		i = encodeVarintRecord(dAtA, i, uint64(len(m.Val)))
		i += copy(dAtA[i:], m.Val)
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *State) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *State) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x8
	i++
	i = encodeVarintRecord(dAtA, i, uint64(m.SchemaVersion))
	dAtA[i] = 0x10
	i++
	i = encodeVarintRecord(dAtA, i, uint64(m.ProcessID))
	dAtA[i] = 0x1a
	i++
	i = encodeVarintRecord(dAtA, i, uint64(len(m.CurrentFile)))
	i += copy(dAtA[i:], m.CurrentFile)
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeVarintRecord(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Record) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovRecord(uint64(m.ProcessID))
	n += 1 + sovRecord(uint64(m.ID))
	n += 1 + sovRecord(uint64(m.PartID))
	n += 2
	n += 1 + sovRecord(uint64(m.Operation))
	n += 1 + sovRecord(uint64(m.Expires))
	if m.Key != nil {
		l = len(m.Key)
		n += 1 + l + sovRecord(uint64(l))
	}
	if m.Val != nil {
		l = len(m.Val)
		n += 1 + l + sovRecord(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *State) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovRecord(uint64(m.SchemaVersion))
	n += 1 + sovRecord(uint64(m.ProcessID))
	l = len(m.CurrentFile)
	n += 1 + l + sovRecord(uint64(l))
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovRecord(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozRecord(x uint64) (n int) {
	return sovRecord(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Record) Unmarshal(dAtA []byte) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRecord
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Record: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Record: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ProcessID", wireType)
			}
			m.ProcessID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ProcessID |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			hasFields[0] |= uint64(0x00000001)
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ID", wireType)
			}
			m.ID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ID |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			hasFields[0] |= uint64(0x00000002)
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field PartID", wireType)
			}
			m.PartID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.PartID |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			hasFields[0] |= uint64(0x00000004)
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field LastPart", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.LastPart = bool(v != 0)
			hasFields[0] |= uint64(0x00000008)
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Operation", wireType)
			}
			m.Operation = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Operation |= (Operation(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			hasFields[0] |= uint64(0x00000010)
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Expires", wireType)
			}
			m.Expires = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Expires |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthRecord
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = append(m.Key[:0], dAtA[iNdEx:postIndex]...)
			if m.Key == nil {
				m.Key = []byte{}
			}
			iNdEx = postIndex
		case 8:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Val", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthRecord
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Val = append(m.Val[:0], dAtA[iNdEx:postIndex]...)
			if m.Val == nil {
				m.Val = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRecord(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRecord
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return github_com_gogo_protobuf_proto.NewRequiredNotSetError("ProcessID")
	}
	if hasFields[0]&uint64(0x00000002) == 0 {
		return github_com_gogo_protobuf_proto.NewRequiredNotSetError("ID")
	}
	if hasFields[0]&uint64(0x00000004) == 0 {
		return github_com_gogo_protobuf_proto.NewRequiredNotSetError("PartID")
	}
	if hasFields[0]&uint64(0x00000008) == 0 {
		return github_com_gogo_protobuf_proto.NewRequiredNotSetError("LastPart")
	}
	if hasFields[0]&uint64(0x00000010) == 0 {
		return github_com_gogo_protobuf_proto.NewRequiredNotSetError("Operation")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *State) Unmarshal(dAtA []byte) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRecord
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: State: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: State: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SchemaVersion", wireType)
			}
			m.SchemaVersion = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.SchemaVersion |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			hasFields[0] |= uint64(0x00000001)
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ProcessID", wireType)
			}
			m.ProcessID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ProcessID |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			hasFields[0] |= uint64(0x00000002)
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CurrentFile", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthRecord
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.CurrentFile = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000004)
		default:
			iNdEx = preIndex
			skippy, err := skipRecord(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRecord
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return github_com_gogo_protobuf_proto.NewRequiredNotSetError("SchemaVersion")
	}
	if hasFields[0]&uint64(0x00000002) == 0 {
		return github_com_gogo_protobuf_proto.NewRequiredNotSetError("ProcessID")
	}
	if hasFields[0]&uint64(0x00000004) == 0 {
		return github_com_gogo_protobuf_proto.NewRequiredNotSetError("CurrentFile")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipRecord(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowRecord
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRecord
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthRecord
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowRecord
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipRecord(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthRecord = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowRecord   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("record.proto", fileDescriptor_record_6547f2e696acbf8b) }

var fileDescriptor_record_6547f2e696acbf8b = []byte{
	// 340 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x6c, 0x90, 0xcd, 0x4a, 0xf3, 0x40,
	0x18, 0x85, 0x3b, 0x93, 0x9f, 0xb6, 0xef, 0xd7, 0x4f, 0xc2, 0xd0, 0xc5, 0x20, 0x12, 0x43, 0x17,
	0x12, 0xba, 0xa8, 0x20, 0xde, 0x80, 0x6d, 0x46, 0x08, 0x16, 0x1b, 0xd2, 0x9f, 0xfd, 0x58, 0x87,
	0x5a, 0x88, 0x4d, 0x98, 0x8c, 0xa8, 0x2b, 0x6f, 0xaf, 0x4b, 0xaf, 0x40, 0xb4, 0xd7, 0xe1, 0x42,
	0x26, 0x4d, 0x6b, 0x0a, 0xee, 0xce, 0x3c, 0xcf, 0xbb, 0x38, 0x73, 0xa0, 0x25, 0xc5, 0x3c, 0x95,
	0xf7, 0xbd, 0x4c, 0xa6, 0x2a, 0x25, 0xd6, 0x33, 0x4f, 0xb2, 0xbb, 0xe3, 0xf6, 0x22, 0x5d, 0xa4,
	0x05, 0x39, 0xd7, 0x69, 0x2b, 0x3b, 0xdf, 0x08, 0xec, 0xb8, 0xb8, 0x26, 0x1d, 0x68, 0x46, 0x32,
	0x9d, 0x8b, 0x3c, 0x0f, 0x03, 0x8a, 0x3c, 0xec, 0x9b, 0x7d, 0x73, 0xfd, 0x71, 0x5a, 0x8b, 0x7f,
	0x31, 0x69, 0x03, 0x0e, 0x03, 0x8a, 0x2b, 0x12, 0x87, 0x01, 0x39, 0x01, 0x3b, 0xe2, 0x52, 0x85,
	0x01, 0x35, 0x3c, 0xec, 0x5b, 0xa5, 0x29, 0x19, 0xf1, 0xa0, 0x31, 0xe4, 0xb9, 0xd2, 0x2f, 0x6a,
	0x7a, 0xd8, 0x6f, 0x94, 0x7e, 0x4f, 0xc9, 0x25, 0x34, 0x47, 0x99, 0x90, 0x5c, 0x2d, 0xd3, 0x15,
	0xb5, 0x3c, 0xec, 0x1f, 0x5d, 0x38, 0xbd, 0xa2, 0x75, 0x6f, 0xcf, 0x77, 0x5d, 0xf6, 0x80, 0xb8,
	0x50, 0x67, 0x2f, 0xd9, 0x52, 0x8a, 0x9c, 0xda, 0x1e, 0xf2, 0x8d, 0xf2, 0x62, 0x07, 0x89, 0x03,
	0xc6, 0x8d, 0x78, 0xa5, 0x75, 0x0f, 0xf9, 0xad, 0x58, 0x47, 0x4d, 0x66, 0x3c, 0xa1, 0x8d, 0x2d,
	0x99, 0xf1, 0xa4, 0xf3, 0x06, 0xd6, 0x58, 0x71, 0x25, 0x48, 0x17, 0xfe, 0x8f, 0xe7, 0x0f, 0xe2,
	0x91, 0xcf, 0x84, 0xcc, 0x75, 0x8d, 0xea, 0x00, 0x87, 0xea, 0x70, 0x28, 0xfc, 0xf7, 0x50, 0x67,
	0xf0, 0x6f, 0xf0, 0x24, 0xa5, 0x58, 0xa9, 0xeb, 0x65, 0x22, 0x8a, 0x5d, 0x9a, 0xe5, 0x55, 0x55,
	0x74, 0x83, 0xca, 0xd7, 0x09, 0x80, 0x3d, 0x88, 0xd9, 0xd5, 0x84, 0x39, 0x35, 0x9d, 0xa7, 0x51,
	0xa0, 0x33, 0x22, 0x75, 0x30, 0xa2, 0xe9, 0xc4, 0xc1, 0x1a, 0x06, 0x6c, 0xc8, 0x26, 0xcc, 0x31,
	0x74, 0x8e, 0xd9, 0x28, 0x62, 0xb7, 0x8e, 0xd9, 0x77, 0xd6, 0x5f, 0x6e, 0x6d, 0xbd, 0x71, 0xd1,
	0xfb, 0xc6, 0x45, 0x9f, 0x1b, 0x17, 0xfd, 0x04, 0x00, 0x00, 0xff, 0xff, 0x15, 0x46, 0x27, 0x4b,
	0x03, 0x02, 0x00, 0x00,
}
