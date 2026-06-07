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
	fmt.Printf("📥 [PUT] Đã ghi nhận Key: %s vào đĩa cứng thành công.\n", req.CaseId)
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

// =========================================================================
// CẬP NHẬT: Hàm xử lý XÓA dữ liệu khỏi LevelDB (Delete) chuẩn hóa Log
// =========================================================================
func (s *server) DeleteCase(ctx context.Context, req *pb.CaseRequest) (*pb.CaseResponse, error) {
	// Kiểm tra Key tồn tại trước khi xóa để log chính xác hơn (Tùy chọn nghiệp vụ)
	hasKey, err := s.db.Has([]byte(req.CaseId), nil)
	if err != nil {
		return &pb.CaseResponse{Success: false, Message: "Lỗi kiểm tra đĩa: " + err.Error()}, nil
	}
	if !hasKey {
		return &pb.CaseResponse{Success: true, Message: "Hồ sơ không tồn tại hoặc đã được loại bỏ trước đó."}, nil
	}

	// Thực hiện xóa cứng dữ liệu
	err = s.db.Delete([]byte(req.CaseId), nil)
	if err != nil {
		return &pb.CaseResponse{Success: false, Message: "Lỗi xóa dữ liệu trên đĩa LevelDB: " + err.Error()}, nil
	}
	fmt.Printf("🗑️ [DELETE] Đã xóa Key: %s khỏi đĩa cứng thành công.\n", req.CaseId)
	return &pb.CaseResponse{Success: true, Message: "Dữ liệu đã được loại bỏ khỏi đĩa an toàn."}, nil
}

func main() {
	// Sử dụng flag để đọc tham số truyền vào từ Terminal
	port := flag.String("port", "50051", "Cổng mạng chạy gRPC Server")
	dbPath := flag.String("db", "db_don_1", "Đường dẫn thư mục lưu trữ GoLevelDB")
	flag.Parse()

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

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Printf("🚨 Lỗi: Cổng mạng %s đã bị chiếm dụng! Vui lòng kiểm tra lại cấu hình phân bổ cổng. %v\n", *port, err)
		db.Close()
		os.Exit(1)
	}

	// Khởi tạo gRPC Server
	s := grpc.NewServer()
	pb.RegisterPoliceStorageServiceServer(s, &server{db: db})

	// =========================================================================
	// TỐI ƯU HOÀN HẢO: Cơ chế Graceful Shutdown đóng cả gRPC và LevelDB an toàn
	// =========================================================================
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n🛑 [HỆ THỐNG] Đang kích hoạt quy trình dừng khẩn cấp an toàn...")

		fmt.Println("1. Ngắt tiếp nhận các yêu cầu gRPC mới và đợi các request cũ kết thúc...")
		s.GracefulStop()

		fmt.Println("2. Đang đóng cơ sở dữ liệu GoLevelDB xuống đĩa cứng...")
		db.Close()

		fmt.Println("💚 Hệ thống đã tắt hoàn toàn an toàn. Dữ liệu không bị ảnh hưởng.")
		os.Exit(0)
	}()

	fmt.Printf("🚀 ĐỒN AN NINH PHÂN TÁN ĐANG CHẠY TẠI CỔNG gRPC: localhost:%s\n", *port)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("🚨 Lỗi khởi chạy gRPC: %v\n", err)
	}
}
