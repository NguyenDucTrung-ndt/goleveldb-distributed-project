package main

import (
	"context"
	"encoding/json"
	"fmt"
	pb "goleveldb-demo/storage"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

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

// Hàm tự động lưu mảng vụ án xuống file json cục bộ để không bị mất khi tắt proxy
func saveCasesToFile() {
	data, _ := json.MarshalIndent(globalCases, "", "  ")
	_ = ioutil.WriteFile("global_cases_cache.json", data, 0644)
}

// Hàm tự động nạp lại vụ án khi bật proxy lên
func loadCasesFromFile() {
	data, err := ioutil.ReadFile("global_cases_cache.json")
	if err == nil {
		_ = json.Unmarshal(data, &globalCases)
	}
}
func main() {
	loadCasesFromFile()
	ring := NewPoliceHashRing(10)
	ring.AddStation("localhost:50051")
	ring.AddStation("localhost:50052")
	r := gin.Default()
	r.LoadHTMLGlob("templates/*")

	r.GET("/", func(c *gin.Context) {
		sort.Slice(globalCases, func(i, j int) bool { return globalCases[i].OccurredAt > globalCases[j].OccurredAt })
		msg := c.Query("msg")

		status1 := checkNodeStatus("127.0.0.1:50051")
		status1Rep := checkNodeStatus("127.0.0.1:50053")
		status2 := checkNodeStatus("127.0.0.1:50052")
		status2Rep := checkNodeStatus("127.0.0.1:50054")

		c.HTML(http.StatusOK, "index.html", gin.H{
			"LoggedIn": isLoggedIn, "Role": currentRole, "FullName": currentFullName,
			"UserList": userDB, "RecentCases": globalCases, "SuccessMsg": msg,
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
				msgLog += ""
			} else {
				msgLog += ""
			}
		} else {
			msgLog += fmt.Sprintf("[Đồn Gốc %s: MẤT ĐIỆN SẬP NGUỒN] ", targetMaster)
		}

		// Thử đồng bộ Replica dự phòng (GIỮ NGUYÊN LOGIC ĐỂ HỆ THỐNG KHÔNG BỊ MẤT TÍNH NĂNG SAO LƯU)
		connR, errR := grpc.Dial(targetReplica, grpc.WithInsecure(), grpc.WithTimeout(500*time.Millisecond))
		if errR == nil {
			defer connR.Close()
			clientR := pb.NewPoliceStorageServiceClient(connR)
			_, errRep := clientR.PutCase(context.Background(), &pb.CaseRequest{CaseId: caseID, CaseDataJson: caseDataJSON})

			if errRep == nil {
				msgLog += ""
			} else {
				msgLog += ""
			}
		} else {
			msgLog += ""
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

		saveCasesToFile()

		c.Redirect(http.StatusSeeOther, "/?msg="+msgLog)
	})
	// =========================================================================
	// HÀM XỬ LÝ LỌC VÀ TRUY VẤN VỤ ÁN ĐA ĐIỀU KIỆN (ROUTE /search ĐỘC LẬP)
	// =========================================================================
	r.GET("/search", func(c *gin.Context) {
		// 1. Lấy dữ liệu từ URL Query gửi lên từ Form HTML
		startTime := c.Query("start_time")
		endTime := c.Query("end_time")
		searchTitle := strings.ToLower(strings.TrimSpace(c.Query("search_title")))

		// 2. Định dạng lại chuỗi thời gian từ HTML (thay chữ T bằng dấu cách để khớp dữ liệu đĩa cứng)
		if startTime != "" {
			startTime = strings.Replace(startTime, "T", " ", 1)
		}
		if endTime != "" {
			endTime = strings.Replace(endTime, "T", " ", 1)
		}

		var filteredCases []CaseSummary

		// 3. Duyệt mảng dữ liệu tổng để sàng lọc
		for _, v := range globalCases {
			match := true

			// Lọc điều kiện 1: Từ ngày (Bỏ qua nếu ô nhập trống)
			if startTime != "" && v.OccurredAt < startTime {
				match = false
			}
			// Lọc điều kiện 2: Đến ngày (Bỏ qua nếu ô nhập trống)
			if endTime != "" && v.OccurredAt > endTime {
				match = false
			}
			// Lọc điều kiện 3: Từ khóa tội danh/hành vi
			if searchTitle != "" {
				inTitle := strings.Contains(strings.ToLower(v.Title), searchTitle)
				inSuspect := strings.Contains(strings.ToLower(v.Suspect), searchTitle)
				inDesc := strings.Contains(strings.ToLower(v.Description), searchTitle)

				// Nếu từ khóa không nằm trong Tiêu đề, Nghi phạm, cũng không có trong Mô tả -> Loại
				if !inTitle && !inSuspect && !inDesc {
					match = false
				}
			}

			// Nếu vượt qua tất cả bộ lọc -> Đưa vào danh sách kết quả
			if match {
				filteredCases = append(filteredCases, v)
			}
		}

		// 4. Trả về giao diện chuyên dụng search.html cùng dữ liệu sau khi lọc
		c.HTML(http.StatusOK, "search.html", gin.H{
			"LoggedIn":    isLoggedIn,
			"Role":        currentRole,
			"FullName":    currentFullName,
			"RecentCases": filteredCases, // Danh sách vụ án đã thu hẹp
			"SuccessMsg":  fmt.Sprintf("Hệ thống tìm thấy %d kết quả phù hợp nghiệp vụ.", len(filteredCases)),

			// Đẩy ngược lại giá trị để giữ chữ trên ô nhập sau khi load trang
			"StartTime":   c.Query("start_time"),
			"EndTime":     c.Query("end_time"),
			"SearchTitle": c.Query("search_title"),
		})
	})
	r.GET("/export", func(c *gin.Context) {
		// 1. Thiết lập Header để trình duyệt hiểu đây là lệnh tải file về máy
		c.Header("Content-Description", "File Transfer")
		c.Header("Content-Disposition", "attachment; filename=Bao_Cao_Ho_So_Vu_An.csv")
		c.Header("Content-Type", "text/csv; charset=utf-8")

		// 2. GIẢI QUYẾT LỖI FONT: Ghi mã BOM (Byte Order Mark) của UTF-8 vào đầu file.
		// Nhờ 3 byte này, khi double-click mở trực tiếp bằng Excel trên Windows sẽ không bị lỗi chữ Tiếng Việt.
		c.Writer.Write([]byte{0xEF, 0xBB, 0xBF})

		// 3. Tạo dòng tiêu đề cột (Header) cho file Excel
		csvContent := "Mã Vụ Án,Ngày Xảy Ra,Tội Danh/Tiêu Đề,Đối Tượng Nghi Vấn,Mô Tả Diễn Biến,Báo Cáo Pháp Y,Vật Chứng Tịch Thu\n"

		// 4. Vòng lặp duyệt qua mảng dữ liệu tổng (đọc từ cache/đĩa cứng LevelDB lên) để ghi từng dòng
		for _, v := range globalCases {

			cleanDesc := strings.ReplaceAll(strings.ReplaceAll(v.Description, "\n", " "), ",", ";")
			cleanAutopsy := strings.ReplaceAll(strings.ReplaceAll(v.AutopsyReport, "\n", " "), ",", ";")
			cleanEvidence := strings.ReplaceAll(strings.ReplaceAll(v.EvidenceList, "\n", " "), ",", ";")
			cleanTitle := strings.ReplaceAll(v.Title, ",", ";")
			cleanSuspect := strings.ReplaceAll(v.Suspect, ",", ";")

			// Nối các trường dữ liệu lại với nhau, ngăn cách bằng dấu phẩy theo chuẩn CSV
			csvContent += fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s\n",
				v.CaseID,
				v.OccurredAt,
				cleanTitle,
				cleanSuspect,
				cleanDesc,
				cleanAutopsy,
				cleanEvidence,
			)
		}

		// 5. Trả luồng dữ liệu thô về cho trình duyệt của người dùng tự động tải xuống
		c.String(http.StatusOK, csvContent)
	})
	r.Run(":8080")
}
