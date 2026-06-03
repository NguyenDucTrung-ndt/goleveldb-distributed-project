package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "goleveldb-demo/storage" // Đảm bảo đường dẫn gói proto này chính xác

	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedPoliceStorageServiceServer
	db *leveldb.DB
}

// Hàm xử lý Ghi dữ liệu (Put)
func (s *server) PutCase(ctx context.Context, req *pb.CaseRequest) (*pb.CaseResponse, error) {
	err := s.db.Put([]byte(req.CaseId), []byte(req.CaseDataJson), nil)
	if err != nil {
		return &pb.CaseResponse{Success: false, Message: "Lỗi hạ đĩa GoLevelDB: " + err.Error()}, nil
	}
	fmt.Printf("📥 [HỆ THỐNG] Đã ghi nhận Key: %s vào đĩa cứng thành công.\n", req.CaseId)
	return &pb.CaseResponse{Success: true, Message: "Dữ liệu đã được lưu trữ an toàn."}, nil
}

// Hàm xử lý Đọc dữ liệu (Get)
func (s *server) GetCase(ctx context.Context, req *pb.CaseRequest) (*pb.CaseResponse, error) {
	data, err := s.db.Get([]byte(req.CaseId), nil)
	if err != nil {
		return &pb.CaseResponse{Success: false, CaseDataJson: "", Message: "Không tìm thấy hồ sơ hoặc lỗi kết nối đĩa."}, nil
	}
	return &pb.CaseResponse{Success: true, CaseDataJson: string(data), Message: "Thành công"}, nil
}

func main() {
	// Sử dụng flag để đọc tham số truyền vào từ Terminal
	port := flag.String("port", "50051", "Cổng mạng chạy gRPC Server")
	dbPath := flag.String("db", "db_don_1", "Đường dẫn thư mục lưu trữ GoLevelDB")
	flag.Parse()

	// Nếu người dùng truyền tham số dạng không có flag (Ví dụ: go run main.go 50054 db_don_2_du_phong)
	args := flag.Args()
	if len(args) >= 2 {
		*port = args[0]
		*dbPath = args[1]
	}

	fmt.Printf("💾 Đang khởi tạo cơ sở dữ liệu LevelDB tại thư mục: %s...\n", *dbPath)
	db, err := leveldb.OpenFile(*dbPath, nil)
	if err != nil {
		fmt.Printf("🚨 Lỗi nghiêm trọng: Không thể mở đĩa LevelDB tại %s. Có thể tiến trình khác đang chiếm giữ đĩa này! Chi tiết: %v\n", *dbPath, err)
		os.Exit(1)
	}

	// Tối ưu: Đóng DB an toàn kể cả khi bấm Ctrl+C đột ngột tránh hỏng file dữ liệu
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\n🛑 Đang đóng cơ sở dữ liệu GoLevelDB an toàn...")
		db.Close()
		os.Exit(0)
	}()

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Printf("🚨 Lỗi: Cổng mạng %s đã bị chiếm dụng! Vui lòng kiểm tra lại cấu hình phân bổ cổng. %v\n", *port, err)
		db.Close()
		os.Exit(1)
	}

	s := grpc.NewServer()
	pb.RegisterPoliceStorageServiceServer(s, &server{db: db})

	fmt.Printf("🚀 ĐỒN AN NINH PHÂN TÁN ĐANG CHẠY TẠI CỔNG gRPC: localhost:%s\n", *port)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("🚨 Lỗi khởi chạy gRPC: %v\n", err)
	}
}
