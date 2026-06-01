package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"sort"
	"strconv"
	"time"

	pb "goleveldb-demo/storage"
	"google.golang.org/grpc"
)

// Cấu trúc vòng tròn băm nhất quán phục vụ phân mảnh (Sharding Ring)
type PoliceHashRing struct {
	vnodes       int               // Số node ảo để phân bổ đều tải dữ liệu trên đĩa
	ring         map[uint32]string // Bản đồ vị trí băm -> Địa chỉ Đồn cảnh sát thật
	sortedHashes []uint32          // Mảng lưu giá trị hash đã sắp xếp để tìm kiếm nhị phân
}

func NewPoliceHashRing(vnodes int) *PoliceHashRing {
	return &PoliceHashRing{
		vnodes: vnodes,
		ring:   make(map[uint32]string),
	}
}

// Thêm một Đồn Cảnh Sát mới vào cụm phân mảnh
func (h *PoliceHashRing) AddStation(stationAddr string) {
	for i := 0; i < h.vnodes; i++ {
		vnodeKey := stationAddr + "#" + strconv.Itoa(i)
		hash := crc32.ChecksumIEEE([]byte(vnodeKey))
		h.ring[hash] = stationAddr
		h.sortedHashes = append(h.sortedHashes, hash)
	}
	sort.Slice(h.sortedHashes, func(i, j int) bool {
		return h.sortedHashes[i] < h.sortedHashes[j]
	})
}

// Định tuyến: Tìm đồn chịu trách nhiệm quản lý cho Mã vụ án (Key)
func (h *PoliceHashRing) RouteCase(caseID string) string {
	if len(h.sortedHashes) == 0 {
		return ""
	}
	hash := crc32.ChecksumIEEE([]byte(caseID))
	idx := sort.Search(len(h.sortedHashes), func(i int) bool {
		return h.sortedHashes[i] >= hash
	})
	if idx == len(h.sortedHashes) {
		idx = 0 // Vòng tròn khép kín
	}
	return h.ring[h.sortedHashes[idx]]
}

func main() {
	// Khởi tạo vòng tròn băm điều phối phân mảnh
	ring := NewPoliceHashRing(10)

	// Giả lập hệ thống có 2 cụm đồn cảnh sát phân mảnh dữ liệu quy mô lớn
	// Đồn 1 chạy cổng 50051, Đồn 2 chạy cổng 50052
	ring.AddStation("localhost:50051")
	ring.AddStation("localhost:50052")

	fmt.Println("=== TRUNG TÂM ĐIỀU HƯỚNG CẢNH SÁT (PROXY SHARDING) SẴN SÀNG ===")

	// Giả lập danh sách hồ sơ vụ án hình sự cần nạp vào hệ thống phân tán
	cases := []struct {
		ID   string
		Data string
	}{
		{"VA_2026_HN001", `{"title": "Trom cap tai san", "suspect": "Nguyen Van A", "status": "Under_Investigation"}`},
		{"VA_2026_HCM002", `{"title": "Co y gay thuong tich", "suspect": "Tran Van B", "status": "Resolved"}`},
		{"VA_2026_HP003", `{"title": "Giao dich trai phep", "suspect": "Le Van C", "status": "Under_Investigation"}`},
		{"VA_2026_DN004", `{"title": "Buon lau hang hoa", "suspect": "Pham Thi D", "status": "Pending"}`},
	}

	// Tiến hành định tuyến phân mảnh tự động bằng Consistent Hashing
	for _, c := range cases {
		// 1. Tính toán xem Vụ án này phải chuyển về Đồn nào lưu trữ
		targetStation := ring.RouteCase(c.ID)
		fmt.Printf("[SHARDING ROUTER] Vụ án %s được định tuyến về phân vùng: %s\n", c.ID, targetStation)

		// 2. Thiết lập kết nối mạng gRPC gửi tới đồn đó để nạp vào GoLevelDB tương ứng
		conn, err := grpc.Dial(targetStation, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
		if err != nil {
			log.Printf("-> LỖI: Không thể kết nối tới đồn %s", targetStation)
			continue
		}
		
		client := pb.NewPoliceStorageServiceClient(conn)
		res, err := client.PutCase(context.Background(), &pb.CaseRequest{
			CaseId:             c.ID,
			CaseDataJson:       c.Data,
			IsReplicationRoute: false, // Dữ liệu gốc từ client
		})
		
		if err == nil && res.Success {
			fmt.Printf("   -> KẾT QUẢ: Đã lưu trữ thành công lên %s\n", targetStation)
		} else {
			log.Printf("   -> KẾT QUẢ: Lưu trữ thất bại")
		}
		conn.Close()
		fmt.Println("---------------------------------------------------------")
	}
}