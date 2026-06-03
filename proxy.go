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

const (
	roleChief  = "chief"
	roleDeputy = "deputy"
	roleViewer = "viewer"
)

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

// HÃ€M KIá»‚M TRA TRáº NG THÃI THá»°C Táº¾ (ÄÃƒ Sá»¬A Lá»–I LAZY CONNECTION Cá»¦A gRPC)
func checkNodeStatus(addr string) string {
	// Sá»­ dá»¥ng grpc.WithBlock() Ä‘á»ƒ Ã©p gRPC pháº£i káº¿t ná»‘i vÃ  báº¯t tay (Handshake) ngay láº­p tá»©c
	// Náº¿u cá»•ng bá»‹ Ä‘Ã³ng hoáº·c máº¥t Ä‘iá»‡n, nÃ³ sáº½ cháº·n láº¡i vÃ  bÃ¡o lá»—i ngay sau 200 mili-giÃ¢y
	conn, err := grpc.Dial(
		addr,
		grpc.WithInsecure(),
		grpc.WithBlock(), // QUAN TRá»ŒNG: Ã‰p kiá»ƒm tra káº¿t ná»‘i váº­t lÃ½ ngay láº­p tá»©c
		grpc.WithTimeout(200*time.Millisecond),
	)

	if err != nil {
		// Náº¿u khÃ´ng káº¿t ná»‘i Ä‘Æ°á»£c (Äá»“n bá»‹ táº¯t/Máº¥t Ä‘iá»‡n) -> Tráº£ vá» OFFLINE ngay
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
	"sep2026":    {Password: "matkhausep", Role: roleChief, FullName: "Thượng tá Nguyễn Văn A (Trưởng Đồn)"},
	"pho_don_01": {Password: "matkhaupho", Role: roleDeputy, FullName: "Thiếu tá Lê Hoàng Nam (Phó Trưởng Đồn)"},

	"chiensi01":   {Password: "matkhau01", Role: roleViewer, FullName: "Trung úy Trần Văn B (Trinh sát Hình sự)"},
	"trinhsat_02": {Password: "matkhau02", Role: roleViewer, FullName: "Đại úy Phạm Minh Hải (Trinh sát Địa bàn)"},
	"phap_y_03":   {Password: "matkhau03", Role: roleViewer, FullName: "Thượng úy Nguyễn Thị Thu (Chuyên viên Pháp y)"},
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

func canAddCase(role string) bool {
	return role == roleChief || role == roleDeputy
}

func canManageUsers(role string) bool {
	return role == roleChief
}

func putCaseToStation(addr, caseID, caseDataJSON string) (bool, string) {
	if addr == "" {
		return false, "Không tìm thấy đồn dự phòng."
	}

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancelDial()

	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return false, fmt.Sprintf("Đồn %s offline.", addr)
	}
	defer conn.Close()

	callCtx, cancelCall := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancelCall()

	client := pb.NewPoliceStorageServiceClient(conn)
	resp, err := client.PutCase(callCtx, &pb.CaseRequest{CaseId: caseID, CaseDataJson: caseDataJSON})
	if err != nil {
		return false, fmt.Sprintf("Đồn %s không ghi được: %v", addr, err)
	}
	if !resp.Success {
		return false, fmt.Sprintf("Đồn %s từ chối ghi: %s", addr, resp.Message)
	}

	return true, fmt.Sprintf("Đồn %s đã ghi thành công.", addr)
}

// HÃ m tá»± Ä‘á»™ng lÆ°u máº£ng vá»¥ Ã¡n xuá»‘ng file json cá»¥c bá»™ Ä‘á»ƒ khÃ´ng bá»‹ máº¥t khi táº¯t proxy
func saveCasesToFile() {
	data, _ := json.MarshalIndent(globalCases, "", "  ")
	_ = ioutil.WriteFile("global_cases_cache.json", data, 0644)
}

// HÃ m tá»± Ä‘á»™ng náº¡p láº¡i vá»¥ Ã¡n khi báº­t proxy lÃªn
func loadCasesFromFile() {
	data, err := ioutil.ReadFile("global_cases_cache.json")
	if err == nil {
		_ = json.Unmarshal(data, &globalCases)
	}
}
func main() {
	loadCasesFromFile()
	ring := NewPoliceHashRing(10)
	ring.AddStation("127.0.0.1:50051")
	ring.AddStation("127.0.0.1:50052")
	r := gin.Default()
	_ = r.SetTrustedProxies(nil)
	r.LoadHTMLGlob("templates/*")

	r.GET("/", func(c *gin.Context) {
		sort.Slice(globalCases, func(i, j int) bool { return globalCases[i].OccurredAt > globalCases[j].OccurredAt })
		msg := c.Query("msg")

		status1 := checkNodeStatus("127.0.0.1:50051")
		status1Rep := checkNodeStatus("127.0.0.1:50053")
		status2 := checkNodeStatus("127.0.0.1:50052")
		status2Rep := checkNodeStatus("127.0.0.1:50054")

		c.HTML(http.StatusOK, "index.html", gin.H{
			"LoggedIn":       isLoggedIn,
			"Role":           currentRole,
			"FullName":       currentFullName,
			"CanAddCase":     canAddCase(currentRole),
			"CanManageUsers": canManageUsers(currentRole),
			"UserList":       userDB,
			"RecentCases":    globalCases,
			"SuccessMsg":     msg,
			"Status1":        status1,
			"Status1Rep":     status1Rep,
			"Status2":        status2,
			"Status2Rep":     status2Rep,
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
		if !isLoggedIn || !canAddCase(currentRole) {
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
		masterOK, masterMsg := putCaseToStation(targetMaster, caseID, caseDataJSON)
		replicaOK, replicaMsg := putCaseToStation(targetReplica, caseID, caseDataJSON)
		if !masterOK && !replicaOK {
			c.String(http.StatusServiceUnavailable, "Không thể thêm hồ sơ: tất cả đồn lưu trữ đang tắt hoặc không phản hồi. %s %s", masterMsg, replicaMsg)
			return
		}
		msgLog := masterMsg + " " + replicaMsg
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
	// HÃ€M Xá»¬ LÃ Lá»ŒC VÃ€ TRUY Váº¤N Vá»¤ ÃN ÄA ÄIá»€U KIá»†N (ROUTE /search Äá»˜C Láº¬P)
	// =========================================================================
	r.GET("/search", func(c *gin.Context) {
		// 1. Láº¥y dá»¯ liá»‡u tá»« URL Query gá»­i lÃªn tá»« Form HTML
		startTime := c.Query("start_time")
		endTime := c.Query("end_time")
		searchTitle := strings.ToLower(strings.TrimSpace(c.Query("search_title")))

		// 2. Äá»‹nh dáº¡ng láº¡i chuá»—i thá»i gian tá»« HTML (thay chá»¯ T báº±ng dáº¥u cÃ¡ch Ä‘á»ƒ khá»›p dá»¯ liá»‡u Ä‘Ä©a cá»©ng)
		if startTime != "" {
			startTime = strings.Replace(startTime, "T", " ", 1)
		}
		if endTime != "" {
			endTime = strings.Replace(endTime, "T", " ", 1)
		}

		var filteredCases []CaseSummary

		// 3. Duyá»‡t máº£ng dá»¯ liá»‡u tá»•ng Ä‘á»ƒ sÃ ng lá»c
		for _, v := range globalCases {
			match := true

			// Lá»c Ä‘iá»u kiá»‡n 1: Tá»« ngÃ y (Bá» qua náº¿u Ã´ nháº­p trá»‘ng)
			if startTime != "" && v.OccurredAt < startTime {
				match = false
			}
			// Lá»c Ä‘iá»u kiá»‡n 2: Äáº¿n ngÃ y (Bá» qua náº¿u Ã´ nháº­p trá»‘ng)
			if endTime != "" && v.OccurredAt > endTime {
				match = false
			}
			// Lá»c Ä‘iá»u kiá»‡n 3: Tá»« khÃ³a tá»™i danh/hÃ nh vi
			if searchTitle != "" {
				inTitle := strings.Contains(strings.ToLower(v.Title), searchTitle)
				inSuspect := strings.Contains(strings.ToLower(v.Suspect), searchTitle)
				inDesc := strings.Contains(strings.ToLower(v.Description), searchTitle)

				// Náº¿u tá»« khÃ³a khÃ´ng náº±m trong TiÃªu Ä‘á», Nghi pháº¡m, cÅ©ng khÃ´ng cÃ³ trong MÃ´ táº£ -> Loáº¡i
				if !inTitle && !inSuspect && !inDesc {
					match = false
				}
			}

			// Náº¿u vÆ°á»£t qua táº¥t cáº£ bá»™ lá»c -> ÄÆ°a vÃ o danh sÃ¡ch káº¿t quáº£
			if match {
				filteredCases = append(filteredCases, v)
			}
		}

		// 4. Tráº£ vá» giao diá»‡n chuyÃªn dá»¥ng search.html cÃ¹ng dá»¯ liá»‡u sau khi lá»c
		c.HTML(http.StatusOK, "search.html", gin.H{
			"LoggedIn":       isLoggedIn,
			"Role":           currentRole,
			"FullName":       currentFullName,
			"CanAddCase":     canAddCase(currentRole),
			"CanManageUsers": canManageUsers(currentRole),
			"RecentCases":    filteredCases, // Danh sÃ¡ch vá»¥ Ã¡n Ä‘Ã£ thu háº¹p
			"SuccessMsg":     fmt.Sprintf("Hệ thống tìm thấy %d kết quả phù hợp nghiệp vụ.", len(filteredCases)),

			// Äáº©y ngÆ°á»£c láº¡i giÃ¡ trá»‹ Ä‘á»ƒ giá»¯ chá»¯ trÃªn Ã´ nháº­p sau khi load trang
			"StartTime":   c.Query("start_time"),
			"EndTime":     c.Query("end_time"),
			"SearchTitle": c.Query("search_title"),
		})
	})
	r.GET("/export", func(c *gin.Context) {
		// 1. Thiáº¿t láº­p Header Ä‘á»ƒ trÃ¬nh duyá»‡t hiá»ƒu Ä‘Ã¢y lÃ  lá»‡nh táº£i file vá» mÃ¡y
		c.Header("Content-Description", "File Transfer")
		c.Header("Content-Disposition", "attachment; filename=Bao_Cao_Ho_So_Vu_An.csv")
		c.Header("Content-Type", "text/csv; charset=utf-8")

		// 2. GIáº¢I QUYáº¾T Lá»–I FONT: Ghi mÃ£ BOM (Byte Order Mark) cá»§a UTF-8 vÃ o Ä‘áº§u file.
		// Nhá» 3 byte nÃ y, khi double-click má»Ÿ trá»±c tiáº¿p báº±ng Excel trÃªn Windows sáº½ khÃ´ng bá»‹ lá»—i chá»¯ Tiáº¿ng Viá»‡t.
		c.Writer.Write([]byte{0xEF, 0xBB, 0xBF})

		// 3. Táº¡o dÃ²ng tiÃªu Ä‘á» cá»™t (Header) cho file Excel
		csvContent := "Mã Vụ Án,Ngày Xảy Ra,Tội Danh/Tiêu Đề,Đối Tượng Nghi Vấn,Mô Tả Diễn Biến,Báo Cáo Pháp Y,Vật Chứng Tịch Thu\n"

		// 4. VÃ²ng láº·p duyá»‡t qua máº£ng dá»¯ liá»‡u tá»•ng (Ä‘á»c tá»« cache/Ä‘Ä©a cá»©ng LevelDB lÃªn) Ä‘á»ƒ ghi tá»«ng dÃ²ng
		for _, v := range globalCases {

			cleanDesc := strings.ReplaceAll(strings.ReplaceAll(v.Description, "\n", " "), ",", ";")
			cleanAutopsy := strings.ReplaceAll(strings.ReplaceAll(v.AutopsyReport, "\n", " "), ",", ";")
			cleanEvidence := strings.ReplaceAll(strings.ReplaceAll(v.EvidenceList, "\n", " "), ",", ";")
			cleanTitle := strings.ReplaceAll(v.Title, ",", ";")
			cleanSuspect := strings.ReplaceAll(v.Suspect, ",", ";")

			// Ná»‘i cÃ¡c trÆ°á»ng dá»¯ liá»‡u láº¡i vá»›i nhau, ngÄƒn cÃ¡ch báº±ng dáº¥u pháº©y theo chuáº©n CSV
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

		// 5. Tráº£ luá»“ng dá»¯ liá»‡u thÃ´ vá» cho trÃ¬nh duyá»‡t cá»§a ngÆ°á»i dÃ¹ng tá»± Ä‘á»™ng táº£i xuá»‘ng
		c.String(http.StatusOK, csvContent)
	})
	r.Run("localhost:8080")
}
