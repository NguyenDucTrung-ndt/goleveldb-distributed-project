package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "goleveldb-demo/storage"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

type UserAccount struct {
	Password string `json:"-"`
	Role     string `json:"role"`
	FullName string `json:"full_name"`
}

type CaseSummary struct {
	CaseID        string `json:"case_id"`
	OccurredAt    string `json:"occurred_at"`
	Title         string `json:"title"`
	Suspect       string `json:"suspect"`
	Description   string `json:"description"`
	AutopsyReport string `json:"autopsy_report"`
	EvidenceList  string `json:"evidence_list"`
}

type PoliceHashRing struct {
	vnodes       int
	ring         map[uint32]string
	sortedHashes []uint32
}

func NewPoliceHashRing(vnodes int) *PoliceHashRing {
	return &PoliceHashRing{vnodes: vnodes, ring: make(map[uint32]string)}
}

func (h *PoliceHashRing) AddStation(stationAddr string) {
	for i := 0; i < h.vnodes; i++ {
		vnodeKey := stationAddr + "#" + strconv.Itoa(i)
		hash := crc32.ChecksumIEEE([]byte(vnodeKey))
		h.ring[hash] = stationAddr
		h.sortedHashes = append(h.sortedHashes, hash)
	}
	sort.Slice(h.sortedHashes, func(i, j int) bool { return h.sortedHashes[i] < h.sortedHashes[j] })
}

func (h *PoliceHashRing) RouteCase(caseID string) string {
	if len(h.sortedHashes) == 0 {
		return ""
	}
	hash := crc32.ChecksumIEEE([]byte(caseID))
	idx := sort.Search(len(h.sortedHashes), func(i int) bool { return h.sortedHashes[i] >= hash })
	if idx == len(h.sortedHashes) {
		idx = 0
	}
	return h.ring[h.sortedHashes[idx]]
}

// Hàm kiểm tra trạng thái sống/chết (Health Check) của một cổng gRPC
// HÀM KIỂM TRA TRẠNG THÁI THỰC TẾ (ĐÃ SỬA LỖI LAZY CONNECTION CỦA gRPC)
func checkNodeStatus(addr string) string {
	// Sử dụng grpc.WithBlock() để ép gRPC phải kết nối và bắt tay (Handshake) ngay lập tức
	// Nếu cổng bị đóng hoặc mất điện, nó sẽ chặn lại và báo lỗi ngay sau 200 mili-giây
	conn, err := grpc.Dial(
		addr,
		grpc.WithInsecure(),
		grpc.WithBlock(), // QUAN TRỌNG: Ép kiểm tra kết nối vật lý ngay lập tức
		grpc.WithTimeout(200*time.Millisecond),
	)

	if err != nil {
		// Nếu không kết nối được (Đồn bị tắt/Mất điện) -> Trả về OFFLINE ngay
		return "OFFLINE"
	}
	defer conn.Close()

	return "ONLINE"
}

var replicationMap = map[string]string{
	"127.0.0.1:50051": "127.0.0.1:50053",
	"127.0.0.1:50052": "127.0.0.1:50054",
}

var userDB = map[string]UserAccount{
	"sep2026":    {Password: "matkhausep", Role: "admin", FullName: "Thượng tá Nguyễn Văn A (Trưởng Phân Khu)"},
	"pho_don_01": {Password: "matkhaupho", Role: "admin", FullName: "Thiếu tá Lê Hoàng Nam (Phó Trưởng Đồn)"},

	"chiensi01":   {Password: "matkhau01", Role: "viewer", FullName: "Trung úy Trần Văn B (Trinh sát Hình sự)"},
	"trinhsat_02": {Password: "matkhau02", Role: "viewer", FullName: "Đại úy Phạm Minh Hải (Trinh sát Địa bàn)"},
	"phap_y_03":   {Password: "matkhau03", Role: "viewer", FullName: "Thượng úy Nguyễn Thị Thu (Chuyên viên Pháp y)"},
}

var globalCases = []CaseSummary{
	{
		CaseID:        "VA_2026_HN001",
		OccurredAt:    "2026-05-15 22:30",
		Title:         "Giết người cướp tài sản công vụ",
		Suspect:       "Nguyễn Văn Hoành",
		Description:   "Đối tượng Hoành đột nhập vào trạm gác lúc nửa đêm, dùng hung khí tấn công chiến sĩ trực ban từ phía sau nhằm lấy đi tài liệu tối mật.",
		AutopsyReport: "Nạn nhân tử vong do mất máu cấp. Vết thương chí mạng sâu 5cm ở vùng gáy do vật sắc nhọn gây ra.",
		EvidenceList:  "01 Cây dao bấm kim loại màu đen có dính vết máu; 01 Xe máy tháo biển số.",
	},
}

var currentRole = ""
var currentFullName = ""
var isLoggedIn = false

func main() {
	ring := NewPoliceHashRing(10)
	ring.AddStation("localhost:50051")
	ring.AddStation("localhost:50052")

	r := gin.Default()
	r.LoadHTMLGlob("templates/*")

	r.GET("/", func(c *gin.Context) {
		sort.Slice(globalCases, func(i, j int) bool { return globalCases[i].OccurredAt > globalCases[j].OccurredAt })
		msg := c.Query("msg")

		// Thực hiện quét trạng thái thực tế của cả 4 đồn ngay khi tải trang
		status1 := checkNodeStatus("127.0.0.1:50051")
		status1Rep := checkNodeStatus("127.0.0.1:50053")
		status2 := checkNodeStatus("127.0.0.1:50052")
		status2Rep := checkNodeStatus("127.0.0.1:50054")

		c.HTML(http.StatusOK, "index.html", gin.H{
			"LoggedIn": isLoggedIn, "Role": currentRole, "FullName": currentFullName,
			"UserList": userDB, "RecentCases": globalCases, "SuccessMsg": msg,
			// Truyền trạng thái quét thực tế sang HTML
			"Status1": status1, "Status1Rep": status1Rep,
			"Status2": status2, "Status2Rep": status2Rep,
		})
	})

	r.POST("/login", func(c *gin.Context) {
		username, password := c.PostForm("username"), c.PostForm("password")
		user, exists := userDB[username]
		if exists && user.Password == password {
			isLoggedIn, currentRole, currentFullName = true, user.Role, user.FullName
			c.Redirect(http.StatusSeeOther, "/")
		} else {
			c.HTML(http.StatusOK, "index.html", gin.H{"LoggedIn": false, "Error": "Sai mật mã an ninh!"})
		}
	})

	r.GET("/logout", func(c *gin.Context) {
		isLoggedIn = false
		c.Redirect(http.StatusSeeOther, "/")
	})

	r.POST("/add", func(c *gin.Context) {
		if !isLoggedIn || currentRole != "admin" {
			c.String(http.StatusForbidden, "Truy cập bị chặn.")
			return
		}

		caseID := c.PostForm("case_id")
		title := c.PostForm("title")
		suspect := c.PostForm("suspect")
		occurredAt := strings.Replace(c.PostForm("occurred_at"), "T", " ", 1)
		description := c.PostForm("description")
		autopsyReport := c.PostForm("autopsy_report")
		evidenceList := c.PostForm("evidence_list")

		caseDataJSON := fmt.Sprintf(`{"title": "%s", "suspect": "%s", "status": "Under_Investigation", "occurred_at": "%s", "description": "%s", "autopsy_report": "%s", "evidence_list": "%s"}`,
			title, suspect, occurredAt, description, autopsyReport, evidenceList)

		targetMaster := ring.RouteCase(caseID)
		targetReplica := replicationMap[targetMaster]
		msgLog := ""

		// Thử ghi Master hình sự
		connM, errM := grpc.Dial(targetMaster, grpc.WithInsecure(), grpc.WithTimeout(500*time.Millisecond))
		if errM == nil {
			defer connM.Close()
			clientM := pb.NewPoliceStorageServiceClient(connM)
			_, errPut := clientM.PutCase(context.Background(), &pb.CaseRequest{CaseId: caseID, CaseDataJson: caseDataJSON})
			if errPut == nil {
				msgLog += fmt.Sprintf("[Đồn Gốc %s: Đã lưu đĩa cứng] ", targetMaster)
			} else {
				msgLog += fmt.Sprintf("[Đồn Gốc %s: Lỗi ghi đĩa] ", targetMaster)
			}
		} else {
			msgLog += fmt.Sprintf("[Đồn Gốc %s: MẤT ĐIỆN SẬP NGUỒN] ", targetMaster)
		}

		// Thử đồng bộ Replica dự phòng
		connR, errR := grpc.Dial(targetReplica, grpc.WithInsecure(), grpc.WithTimeout(500*time.Millisecond))
		if errR == nil {
			defer connR.Close()
			clientR := pb.NewPoliceStorageServiceClient(connR)
			_, errRep := clientR.PutCase(context.Background(), &pb.CaseRequest{CaseId: caseID, CaseDataJson: caseDataJSON})
			if errRep == nil {
				msgLog += fmt.Sprintf("-> [Đồng bộ cứu hộ sang Đồn Dự phòng %s thành công!]", targetReplica)
			} else {
				msgLog += "-> [Đồn Dự phòng từ chối lệnh sao lưu]"
			}
		} else {
			msgLog += "-> [Đồn Dự phòng cũng đã sập, mất liên lạc hoàn toàn]"
		}

		globalCases = append(globalCases, CaseSummary{
			CaseID:        caseID,
			OccurredAt:    occurredAt,
			Title:         title,
			Suspect:       suspect,
			Description:   description,
			AutopsyReport: autopsyReport,
			EvidenceList:  evidenceList,
		})

		c.Redirect(http.StatusSeeOther, "/?msg="+msgLog)
	})

	r.Run(":8080")
}
