package storage

import (
	"context"
	"google.golang.org/grpc"
)

// Khai báo cấu trúc dữ liệu hồ sơ vụ án
type CaseRequest struct {
	CaseId             string `protobuf:"bytes,1,opt,name=case_id,json=caseId,proto3" json:"case_id,omitempty"`
	CaseDataJson       string `protobuf:"bytes,2,opt,name=case_data_json,json=caseDataJson,proto3" json:"case_data_json,omitempty"`
	IsReplicationRoute bool   `protobuf:"varint,3,opt,name=is_replication_route,json=isReplicationRoute,proto3" json:"is_replication_route,omitempty"`
}

type GetCaseRequest struct {
	CaseId string `protobuf:"bytes,1,opt,name=case_id,json=caseId,proto3" json:"case_id,omitempty"`
}

type GetCaseResponse struct {
	CaseDataJson string `protobuf:"bytes,1,opt,name=case_data_json,json=caseDataJson,proto3" json:"case_data_json,omitempty"`
	Success      bool   `protobuf:"varint,2,opt,name=success,proto3" json:"success,omitempty"`
}

type Response struct {
	Success bool   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

// Định nghĩa Interface cho Client
type PoliceStorageServiceClient interface {
	PutCase(ctx context.Context, in *CaseRequest, opts ...grpc.CallOption) (*Response, error)
	GetCase(ctx context.Context, in *GetCaseRequest, opts ...grpc.CallOption) (*GetCaseResponse, error)
}

type policeStorageServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewPoliceStorageServiceClient(cc grpc.ClientConnInterface) PoliceStorageServiceClient {
	return &policeStorageServiceClient{cc}
}

func (c *policeStorageServiceClient) PutCase(ctx context.Context, in *CaseRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/storage.PoliceStorageService/PutCase", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *policeStorageServiceClient) GetCase(ctx context.Context, in *GetCaseRequest, opts ...grpc.CallOption) (*GetCaseResponse, error) {
	out := new(GetCaseResponse)
	err := c.cc.Invoke(ctx, "/storage.PoliceStorageService/GetCase", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Định nghĩa Interface cho Server
type PoliceStorageServiceServer interface {
	PutCase(context.Context, *CaseRequest) (*Response, error)
	GetCase(context.Context, *GetCaseRequest) (*GetCaseResponse, error)
	mustEmbedUnimplementedPoliceStorageServiceServer()
}

type UnimplementedPoliceStorageServiceServer struct{}

func (UnimplementedPoliceStorageServiceServer) PutCase(context.Context, *CaseRequest) (*Response, error) {
	return nil, nil
}
func (UnimplementedPoliceStorageServiceServer) GetCase(context.Context, *GetCaseRequest) (*GetCaseResponse, error) {
	return nil, nil
}
func (UnimplementedPoliceStorageServiceServer) mustEmbedUnimplementedPoliceStorageServiceServer() {}

func RegisterPoliceStorageServiceServer(s grpc.ServiceRegistrar, srv PoliceStorageServiceServer) {
	s.RegisterService(&_PoliceStorageService_serviceDesc, srv)
}

func _PoliceStorageService_PutCase_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CaseRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PoliceStorageServiceServer).PutCase(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/storage.PoliceStorageService/PutCase",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PoliceStorageServiceServer).PutCase(ctx, req.(*CaseRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PoliceStorageService_GetCase_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetCaseRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PoliceStorageServiceServer).GetCase(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/storage.PoliceStorageService/GetCase",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PoliceStorageServiceServer).GetCase(ctx, req.(*GetCaseRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _PoliceStorageService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "storage.PoliceStorageService",
	HandlerType: (*PoliceStorageServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "PutCase",
			Handler:    _PoliceStorageService_PutCase_Handler,
		},
		{
			MethodName: "GetCase",
			Handler:    _PoliceStorageService_GetCase_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "storage/storage.proto",
}