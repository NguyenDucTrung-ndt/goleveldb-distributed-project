package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"

	// Thay đổi đường dẫn import này khớp với tên go mod của bạn
	pb "goleveldb-demo/storage"
)

type PoliceServer struct {
	pb.UnimplementedPoliceStorageServiceServer
	db           *leveldb.DB
	replicaNodes []string // Danh sách IP/Cổng của các Đồn dự phòng để nhân bản dữ liệu
}

// Hàm xử lý việc Ghi mới hoặc Cập nhật hồ sơ vụ án
func (s *PoliceServer) PutCase(ctx context.Context, req *pb.CaseRequest) (*pb.Response, error) {
	// 1. Thực hiện ghi dữ liệu thô vào đĩa cứng GoLevelDB
	err := s.db.Put([]byte(req.CaseId), []byte(req.CaseDataJson), nil)
	if err != nil {
		log.Printf("[LỖI CORE LEVELDB] Không thể ghi khóa %s xuống đĩa: %v\n", req.CaseId, err)
		return &pb.Response{Success: false, Message: "Lỗi ghi đĩa cục bộ"}, nil
	}
	
	// Log này phải hiện ra ở Terminal của Đồn thì mới chắc chắn dữ liệu đã vào DB
	fmt.Printf("[NODE LOG] Đã ghi hồ sơ vụ án vào GoLevelDB thành công: %s\n", req.CaseId)

	// 2. Cơ chế nhân bản (Replication) sao lưu bất đồng bộ
	if !req.IsReplicationRoute {
		for _, targetAddr := range s.replicaNodes {
			go func(addr string) {
				conn, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					return
				}
				defer conn.Close()

				client := pb.NewPoliceStorageServiceClient(conn)
				_, _ = client.PutCase(context.Background(), &pb.CaseRequest{
					CaseId:             req.CaseId,
					CaseDataJson:       req.CaseDataJson,
					IsReplicationRoute: true, // Chặn vòng lặp vô hạn
				})
			}(targetAddr)
		}
	}

	// TRẢ VỀ SUCCESS = TRUE (Bắt buộc phải có để Proxy nhận diện thành công)
	return &pb.Response{Success: true, Message: "Thành công"}, nil
}

// Hàm xử lý việc Đọc/Truy xuất hồ sơ vụ án
func (s *PoliceServer) GetCase(ctx context.Context, req *pb.GetCaseRequest) (*pb.GetCaseResponse, error) {
	val, err := s.db.Get([]byte(req.CaseId), nil)
	if err != nil {
		return &pb.GetCaseResponse{Success: false, CaseDataJson: ""}, nil
	}
	return &pb.GetCaseResponse{Success: true, CaseDataJson: string(val)}, nil
}

func main() {
	// Lấy tham số cổng và thư mục DB từ dòng lệnh khi chạy để test nhiều Node trên 1 máy
	// Cú pháp chạy test: go run main.go [Cổng gRPC] [Thư mục lưu trữ DB] [Cổng Node nhân bản nếu có]
	if len(os.Args) < 3 {
		log.Fatalf("Sử dụng: go run main.go [Port] [DB_Folder] [Replica_Port_1] [Replica_Port_2]...")
	}
	port := os.Args[1]
	dbFolder := os.Args[2]

	// Thu thập các node slave cần nhân bản dữ liệu
	var replicas []string
	if len(os.Args) > 3 {
		for i := 3; i < len(os.Args); i++ {
			replicas = append(replicas, "localhost:"+os.Args[i])
		}
	}

	// Khởi mở thư viện GoLevelDB nhúng
	db, err := leveldb.OpenFile(dbFolder, nil)
	if err != nil {
		log.Fatalf("Không thể khởi động phân vùng lưu trữ GoLevelDB: %v", err)
	}
	defer db.Close()

	// Khởi tạo mạng gRPC Server cho Đồn Cảnh Sát
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Lỗi mở cổng mạng: %v", err)
	}

	grpcServer := grpc.NewServer()
	server := &PoliceServer{
		db:           db,
		replicaNodes: replicas,
	}
	pb.RegisterPoliceStorageServiceServer(grpcServer, server)

	fmt.Printf("=== ĐỒN CẢNH SÁT ĐANG HOẠT ĐỘNG TẠI CỔNG :%s (Lưu trữ: %s) ===\n", port, dbFolder)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Lỗi vận hành gRPC: %v", err)
	}
}