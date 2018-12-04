// Code generated by protoc-gen-go. DO NOT EDIT.
// source: compact_formats.proto

package proto

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// CompactBlock is a packaging of ONLY the data from a block that's needed to:
//   1. Detect a payment to your shielded Sapling address
//   2. Detect a spend of your shielded Sapling notes
//   3. Update your witnesses to generate new Sapling spend proofs.
type CompactBlock struct {
	ProtoVersion         uint32       `protobuf:"varint,1,opt,name=protoVersion,proto3" json:"protoVersion,omitempty"`
	Height               uint64       `protobuf:"varint,2,opt,name=height,proto3" json:"height,omitempty"`
	Hash                 []byte       `protobuf:"bytes,3,opt,name=hash,proto3" json:"hash,omitempty"`
	Time                 uint32       `protobuf:"varint,4,opt,name=time,proto3" json:"time,omitempty"`
	Header               []byte       `protobuf:"bytes,5,opt,name=header,proto3" json:"header,omitempty"`
	Vtx                  []*CompactTx `protobuf:"bytes,6,rep,name=vtx,proto3" json:"vtx,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *CompactBlock) Reset()         { *m = CompactBlock{} }
func (m *CompactBlock) String() string { return proto.CompactTextString(m) }
func (*CompactBlock) ProtoMessage()    {}
func (*CompactBlock) Descriptor() ([]byte, []int) {
	return fileDescriptor_dce29fee3ee34899, []int{0}
}

func (m *CompactBlock) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CompactBlock.Unmarshal(m, b)
}
func (m *CompactBlock) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CompactBlock.Marshal(b, m, deterministic)
}
func (m *CompactBlock) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CompactBlock.Merge(m, src)
}
func (m *CompactBlock) XXX_Size() int {
	return xxx_messageInfo_CompactBlock.Size(m)
}
func (m *CompactBlock) XXX_DiscardUnknown() {
	xxx_messageInfo_CompactBlock.DiscardUnknown(m)
}

var xxx_messageInfo_CompactBlock proto.InternalMessageInfo

func (m *CompactBlock) GetProtoVersion() uint32 {
	if m != nil {
		return m.ProtoVersion
	}
	return 0
}

func (m *CompactBlock) GetHeight() uint64 {
	if m != nil {
		return m.Height
	}
	return 0
}

func (m *CompactBlock) GetHash() []byte {
	if m != nil {
		return m.Hash
	}
	return nil
}

func (m *CompactBlock) GetTime() uint32 {
	if m != nil {
		return m.Time
	}
	return 0
}

func (m *CompactBlock) GetHeader() []byte {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *CompactBlock) GetVtx() []*CompactTx {
	if m != nil {
		return m.Vtx
	}
	return nil
}

type CompactTx struct {
	// Index and hash will allow the receiver to call out to chain
	// explorers or other data structures to retrieve more information
	// about this transaction.
	Index uint64 `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	Hash  []byte `protobuf:"bytes,2,opt,name=hash,proto3" json:"hash,omitempty"`
	// The transaction fee: present if server can provide. In the case of a
	// stateless server and a transaction with transparent inputs, this will be
	// unset because the calculation requires reference to prior transactions.
	// in a pure-Sapling context, the fee will be calculable as:
	//    valueBalance + (sum(vPubNew) - sum(vPubOld) - sum(tOut))
	Fee                  uint32           `protobuf:"varint,3,opt,name=fee,proto3" json:"fee,omitempty"`
	Spends               []*CompactSpend  `protobuf:"bytes,4,rep,name=spends,proto3" json:"spends,omitempty"`
	Outputs              []*CompactOutput `protobuf:"bytes,5,rep,name=outputs,proto3" json:"outputs,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *CompactTx) Reset()         { *m = CompactTx{} }
func (m *CompactTx) String() string { return proto.CompactTextString(m) }
func (*CompactTx) ProtoMessage()    {}
func (*CompactTx) Descriptor() ([]byte, []int) {
	return fileDescriptor_dce29fee3ee34899, []int{1}
}

func (m *CompactTx) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CompactTx.Unmarshal(m, b)
}
func (m *CompactTx) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CompactTx.Marshal(b, m, deterministic)
}
func (m *CompactTx) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CompactTx.Merge(m, src)
}
func (m *CompactTx) XXX_Size() int {
	return xxx_messageInfo_CompactTx.Size(m)
}
func (m *CompactTx) XXX_DiscardUnknown() {
	xxx_messageInfo_CompactTx.DiscardUnknown(m)
}

var xxx_messageInfo_CompactTx proto.InternalMessageInfo

func (m *CompactTx) GetIndex() uint64 {
	if m != nil {
		return m.Index
	}
	return 0
}

func (m *CompactTx) GetHash() []byte {
	if m != nil {
		return m.Hash
	}
	return nil
}

func (m *CompactTx) GetFee() uint32 {
	if m != nil {
		return m.Fee
	}
	return 0
}

func (m *CompactTx) GetSpends() []*CompactSpend {
	if m != nil {
		return m.Spends
	}
	return nil
}

func (m *CompactTx) GetOutputs() []*CompactOutput {
	if m != nil {
		return m.Outputs
	}
	return nil
}

type CompactSpend struct {
	Nf                   []byte   `protobuf:"bytes,1,opt,name=nf,proto3" json:"nf,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CompactSpend) Reset()         { *m = CompactSpend{} }
func (m *CompactSpend) String() string { return proto.CompactTextString(m) }
func (*CompactSpend) ProtoMessage()    {}
func (*CompactSpend) Descriptor() ([]byte, []int) {
	return fileDescriptor_dce29fee3ee34899, []int{2}
}

func (m *CompactSpend) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CompactSpend.Unmarshal(m, b)
}
func (m *CompactSpend) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CompactSpend.Marshal(b, m, deterministic)
}
func (m *CompactSpend) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CompactSpend.Merge(m, src)
}
func (m *CompactSpend) XXX_Size() int {
	return xxx_messageInfo_CompactSpend.Size(m)
}
func (m *CompactSpend) XXX_DiscardUnknown() {
	xxx_messageInfo_CompactSpend.DiscardUnknown(m)
}

var xxx_messageInfo_CompactSpend proto.InternalMessageInfo

func (m *CompactSpend) GetNf() []byte {
	if m != nil {
		return m.Nf
	}
	return nil
}

type CompactOutput struct {
	Cmu                  []byte   `protobuf:"bytes,1,opt,name=cmu,proto3" json:"cmu,omitempty"`
	Epk                  []byte   `protobuf:"bytes,2,opt,name=epk,proto3" json:"epk,omitempty"`
	Ciphertext           []byte   `protobuf:"bytes,3,opt,name=ciphertext,proto3" json:"ciphertext,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CompactOutput) Reset()         { *m = CompactOutput{} }
func (m *CompactOutput) String() string { return proto.CompactTextString(m) }
func (*CompactOutput) ProtoMessage()    {}
func (*CompactOutput) Descriptor() ([]byte, []int) {
	return fileDescriptor_dce29fee3ee34899, []int{3}
}

func (m *CompactOutput) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CompactOutput.Unmarshal(m, b)
}
func (m *CompactOutput) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CompactOutput.Marshal(b, m, deterministic)
}
func (m *CompactOutput) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CompactOutput.Merge(m, src)
}
func (m *CompactOutput) XXX_Size() int {
	return xxx_messageInfo_CompactOutput.Size(m)
}
func (m *CompactOutput) XXX_DiscardUnknown() {
	xxx_messageInfo_CompactOutput.DiscardUnknown(m)
}

var xxx_messageInfo_CompactOutput proto.InternalMessageInfo

func (m *CompactOutput) GetCmu() []byte {
	if m != nil {
		return m.Cmu
	}
	return nil
}

func (m *CompactOutput) GetEpk() []byte {
	if m != nil {
		return m.Epk
	}
	return nil
}

func (m *CompactOutput) GetCiphertext() []byte {
	if m != nil {
		return m.Ciphertext
	}
	return nil
}

func init() {
	proto.RegisterType((*CompactBlock)(nil), "proto.CompactBlock")
	proto.RegisterType((*CompactTx)(nil), "proto.CompactTx")
	proto.RegisterType((*CompactSpend)(nil), "proto.CompactSpend")
	proto.RegisterType((*CompactOutput)(nil), "proto.CompactOutput")
}

func init() { proto.RegisterFile("compact_formats.proto", fileDescriptor_dce29fee3ee34899) }

var fileDescriptor_dce29fee3ee34899 = []byte{
	// 299 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x54, 0x90, 0xcd, 0x4e, 0x84, 0x30,
	0x14, 0x85, 0x53, 0xfe, 0x8c, 0x57, 0xc6, 0x4c, 0xea, 0x68, 0xba, 0x9a, 0x90, 0xae, 0x48, 0x4c,
	0x66, 0xa1, 0x6f, 0xa0, 0x0f, 0x60, 0xd2, 0x31, 0x6e, 0x0d, 0x42, 0x11, 0x32, 0x42, 0x1b, 0x5a,
	0x0c, 0x0f, 0xe4, 0xda, 0x67, 0x34, 0xbd, 0x54, 0x32, 0xac, 0x38, 0xf7, 0x9c, 0xd3, 0xf6, 0xe3,
	0xc2, 0x6d, 0xa9, 0x3a, 0x5d, 0x94, 0xf6, 0xbd, 0x56, 0x43, 0x57, 0x58, 0x73, 0xd0, 0x83, 0xb2,
	0x8a, 0xc6, 0xf8, 0xe1, 0xbf, 0x04, 0xd2, 0xe7, 0xb9, 0xf0, 0xf4, 0xa5, 0xca, 0x13, 0xe5, 0x90,
	0x62, 0xf2, 0x26, 0x07, 0xd3, 0xaa, 0x9e, 0x91, 0x8c, 0xe4, 0x1b, 0xb1, 0xf2, 0xe8, 0x1d, 0x24,
	0x8d, 0x6c, 0x3f, 0x1b, 0xcb, 0x82, 0x8c, 0xe4, 0x91, 0xf0, 0x13, 0xa5, 0x10, 0x35, 0x85, 0x69,
	0x58, 0x98, 0x91, 0x3c, 0x15, 0xa8, 0x9d, 0x67, 0xdb, 0x4e, 0xb2, 0x08, 0xef, 0x41, 0x3d, 0x9f,
	0x2f, 0x2a, 0x39, 0xb0, 0x18, 0x9b, 0x7e, 0xa2, 0x1c, 0xc2, 0x6f, 0x3b, 0xb1, 0x24, 0x0b, 0xf3,
	0xab, 0x87, 0xed, 0x0c, 0x7a, 0xf0, 0x74, 0xaf, 0x93, 0x70, 0x21, 0xff, 0x21, 0x70, 0xb9, 0x58,
	0x74, 0x07, 0x71, 0xdb, 0x57, 0x72, 0x42, 0xcc, 0x48, 0xcc, 0xc3, 0xc2, 0x11, 0x9c, 0x71, 0x6c,
	0x21, 0xac, 0xa5, 0x44, 0xb4, 0x8d, 0x70, 0x92, 0xde, 0x43, 0x62, 0xb4, 0xec, 0x2b, 0xc3, 0x22,
	0x7c, 0xf0, 0x66, 0xfd, 0xe0, 0xd1, 0x65, 0xc2, 0x57, 0xe8, 0x01, 0x2e, 0xd4, 0x68, 0xf5, 0x68,
	0x0d, 0x8b, 0xb1, 0xbd, 0x5b, 0xb7, 0x5f, 0x30, 0x14, 0xff, 0x25, 0xbe, 0x5f, 0xd6, 0x8a, 0xf7,
	0xd0, 0x6b, 0x08, 0xfa, 0x1a, 0x29, 0x53, 0x11, 0xf4, 0x35, 0x3f, 0xc2, 0x66, 0x75, 0xd2, 0xf1,
	0x95, 0xdd, 0xe8, 0x1b, 0x4e, 0x3a, 0x47, 0xea, 0x93, 0xff, 0x09, 0x27, 0xe9, 0x1e, 0xa0, 0x6c,
	0x75, 0x23, 0x07, 0x2b, 0x27, 0xeb, 0xb7, 0x7c, 0xe6, 0x7c, 0x24, 0x88, 0xf4, 0xf8, 0x17, 0x00,
	0x00, 0xff, 0xff, 0xce, 0xfe, 0x90, 0x6d, 0xf3, 0x01, 0x00, 0x00,
}
