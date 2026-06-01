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

type PoliceHashRing struct {
	vnodes       int               
	ring         map[uint32]string 
	sortedHashes []uint32          
}

func NewPoliceHashRing(vnodes int) *PoliceHashRing {
	return &PoliceHashRing{
		vnodes: vnodes,
		ring:   make(map[uint32]string),
	}
}

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

func (h *PoliceHashRing) RemoveStation(stationAddr string) {
	newSortedHashes := make([]uint32, 0)
	for hash, addr := range h.ring {
		if addr == stationAddr {
			delete(h.ring, hash)
		} else {
			newSortedHashes = append(newSortedHashes, hash)
		}
	}
	sort.Slice(newSortedHashes, func(i, j int) bool {
		return newSortedHashes[i] < newSortedHashes[j]
	})
	h.sortedHashes = newSortedHashes
}

func (h *PoliceHashRing) RouteCase(caseID string) string {
	if len(h.sortedHashes) == 0 {
		return ""
	}
	hash := crc32.ChecksumIEEE([]byte(caseID))
	idx := sort.Search(len(h.sortedHashes), func(i int) bool {
		return h.sortedHashes[i] >= hash
	})
	if idx == len(h.sortedHashes) {
		idx = 0 
	}
	return h.ring[h.sortedHashes[idx]]
}

func main() {
	ring := NewPoliceHashRing(10)
	ring.AddStation("localhost:50051")
	ring.AddStation("localhost:50052")

	fmt.Println("=== TRUNG TÂM ĐIỀU HƯỚNG CẢNH SÁT MỞ RỘNG (FAILOVER & READ ROUTING) ===")

	cases := []struct {
		ID   string
		Data string
	}{
		{"VA_2026_HN001", `{"title": "Trom cap tai san", "suspect": "Nguyen Van A", "status": "Under_Investigation"}`},
		{"VA_2026_HCM002", `{"title": "Co y gay thuong tich", "suspect": "Tran Van B", "status": "Resolved"}`},
		{"VA_2026_HP003", `{"title": "Giao dich trai phep", "suspect": "Le Van C", "status": "Under_Investigation"}`},
		{"VA_2026_DN004", `{"title": "Buon lau hang hoa", "suspect": "Pham Thi D", "status": "Pending"}`},
	}

	fmt.Println("\n>>> TIẾN TRÌNH 1: GHI DỮ LIỆU (WRITE SHARDING WITH FAILOVER) <<<")
	fmt.Println("-----------------------------------------------------------------")

	for _, c := range cases {
		targetStation := ring.RouteCase(c.ID)
		fmt.Printf("[ROUTER] Vụ án %s ban đầu thuộc về Đồn: %s\n", c.ID, targetStation)

		conn, err := grpc.Dial(targetStation, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
		
		if err != nil {
			fmt.Printf("[⚠️ SỰ CỐ] Đồn %s không phản hồi (Mất điện/Sập nguồn)!\n", targetStation)
			fmt.Printf("[FAILOVER] Tự động cô lập đồn %s và tính toán lại tuyến đường...\n", targetStation)
			
			ring.RemoveStation(targetStation)
			targetStation = ring.RouteCase(c.ID)
			fmt.Printf("[FAILOVER] Tuyến đường mới được xác lập chuyển hướng về Đồn: %s\n", targetStation)
			
			conn, err = grpc.Dial(targetStation, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
			if err != nil {
				log.Printf("-> [THẤT BẠI LỚN] Cả cụm đồn thay thế cũng sập!")
				continue
			}
		}

		client := pb.NewPoliceStorageServiceClient(conn)
		
		// Đổi từ dấu gạch dưới (_) thành biến `putRes` để hứng dữ liệu trả về từ Đồn
		putRes, err := client.PutCase(context.Background(), &pb.CaseRequest{
			CaseId:             c.ID,
			CaseDataJson:       c.Data,
			IsReplicationRoute: false,
		})

		// KIỂM TRA: Phải vừa không có lỗi mạng (err == nil) vừa được Đồn xác nhận (putRes.Success == true)
		if err == nil && putRes != nil && putRes.Success {
			fmt.Printf("   -> [THÀNH CÔNG] Hồ sơ đã hạ đĩa an toàn tại: %s\n", targetStation)
		} else {
			log.Printf("   -> [THẤT BẠI] Đồn từ chối ghi hoặc lỗi mạng gRPC. Err: %v", err)
		}
		conn.Close()
		fmt.Println("-----------------------------------------------------------------")
	}

	time.Sleep(1 * time.Second)

	fmt.Println("\n>>> TIẾN TRÌNH 2: TRUY VẤN DỮ LIỆU TẬP TRUNG (CENTRALIZED READ ROUTING) <<<")
	fmt.Println("-----------------------------------------------------------------")
	
	searchKeys := []string{"VA_2026_HN001", "VA_2026_DN004", "KEY_GIA_9999"}

	for _, key := range searchKeys {
		readStation := ring.RouteCase(key)
		fmt.Printf("[READ ROUTER] Đang truy vấn vị trí lưu trữ của mã %s... -> Đồn phụ trách: %s\n", key, readStation)

		conn, err := grpc.Dial(readStation, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
		if err != nil {
			fmt.Printf("   -> [LỖI MẠNG] Không thể kết nối đến đồn đọc dữ liệu %s\n", readStation)
			continue
		}

		client := pb.NewPoliceStorageServiceClient(conn)
		getRes, getErr := client.GetCase(context.Background(), &pb.GetCaseRequest{CaseId: key})

		if getErr == nil && getRes.Success {
			fmt.Printf("   -> [KẾT QUẢ ĐỌC ĐĨA]: Tìm thấy hồ sơ! Dữ liệu: %s\n", getRes.CaseDataJson)
		} else {
			fmt.Printf("   -> [KẾT QUẢ ĐỌC ĐĨA]: ❌ Không tồn tại hồ sơ vụ án này trong hệ thống!\n")
		}
		conn.Close()
		fmt.Println("-----------------------------------------------------------------")
	}
}