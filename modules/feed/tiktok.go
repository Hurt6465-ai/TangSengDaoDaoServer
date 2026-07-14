package feed

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var tiktokVideoPattern = regexp.MustCompile(`/@[^/]+/video/([0-9]{8,32})`)
var tiktokLooseVideoPattern = regexp.MustCompile(`(?:/video/|/v/)([0-9]{8,32})`)
var tiktokURLInTextPattern = regexp.MustCompile(`(?i)(?:https?://)?(?:[a-z0-9-]+\.)*tiktok\.com/[^\s]+`)

var tiktokTransport = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	DialContext:           (&net.Dialer{Timeout: 4 * time.Second, KeepAlive: 20 * time.Second}).DialContext,
	TLSHandshakeTimeout:   4 * time.Second,
	ResponseHeaderTimeout: 5 * time.Second,
	MaxIdleConns:          20,
	MaxIdleConnsPerHost:   4,
	IdleConnTimeout:       30 * time.Second,
}

func resolveTikTok(rawURL string) (*TikTokPreviewResp, error) {
	normalized, videoID, err := normalizeTikTokURL(rawURL)
	if err != nil {
		return nil, err
	}
	endpoint := "https://www.tiktok.com/oembed?url=" + url.QueryEscape(normalized)
	client := safeTikTokClient(8 * time.Second)
	req, _ := http.NewRequest(http.MethodGet, endpoint, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; Talkami/1.0)")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("暂时无法读取TikTok视频")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("TikTok视频不存在或不可公开嵌入")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil, fmt.Errorf("读取TikTok信息失败")
	}
	var result struct {
		Title        string `json:"title"`
		AuthorName   string `json:"author_name"`
		ThumbnailURL string `json:"thumbnail_url"`
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("TikTok返回数据格式异常")
	}
	cover := strings.TrimSpace(result.ThumbnailURL)
	if cover == "" || !isHTTPSURL(cover) {
		return nil, fmt.Errorf("TikTok封面暂时不可用")
	}
	return &TikTokPreviewResp{
		Provider:   "tiktok",
		VideoID:    videoID,
		URL:        normalized,
		EmbedURL:   "https://www.tiktok.com/player/v1/" + videoID,
		CoverURL:   cover,
		Title:      truncateRunes(strings.TrimSpace(result.Title), 500),
		AuthorName: truncateRunes(strings.TrimSpace(result.AuthorName), 200),
	}, nil
}

func normalizeTikTokURL(rawURL string) (string, string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if candidate := tiktokURLInTextPattern.FindString(rawURL); candidate != "" {
		rawURL = strings.Trim(candidate, "\"'()[]{}<>，。！？!?；;,")
	}
	if rawURL == "" {
		return "", "", fmt.Errorf("请输入TikTok链接")
	}
	if !strings.Contains(rawURL, "://") {
		rawURL = "https://" + rawURL
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || !isAllowedTikTokHost(parsed.Hostname()) {
		return "", "", fmt.Errorf("仅支持TikTok官方视频链接")
	}
	parsed.Scheme = "https"
	parsed.User = nil
	parsed.Fragment = ""
	finalURL := parsed
	if needsTikTokRedirect(parsed) {
		client := safeTikTokClient(6 * time.Second)
		req, _ := http.NewRequest(http.MethodGet, parsed.String(), nil)
		req.Header.Set("Range", "bytes=0-0")
		req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; Talkami/1.0)")
		resp, requestErr := client.Do(req)
		if requestErr != nil {
			return "", "", fmt.Errorf("TikTok短链接解析失败")
		}
		if resp.Body != nil {
			resp.Body.Close()
		}
		finalURL = resp.Request.URL
		if !isAllowedTikTokHost(finalURL.Hostname()) {
			return "", "", fmt.Errorf("TikTok链接跳转地址无效")
		}
	}
	match := tiktokVideoPattern.FindStringSubmatch(finalURL.Path)
	if len(match) < 2 {
		match = tiktokLooseVideoPattern.FindStringSubmatch(finalURL.Path)
	}
	if len(match) < 2 {
		return "", "", fmt.Errorf("未识别到TikTok视频ID")
	}
	videoID := match[1]
	finalURL.RawQuery = ""
	finalURL.Fragment = ""
	finalURL.Scheme = "https"
	host := strings.ToLower(finalURL.Hostname())
	if host == "m.tiktok.com" || host == "tiktok.com" {
		host = "www.tiktok.com"
	}
	finalURL.Host = host
	finalURL.User = nil
	return finalURL.String(), videoID, nil
}

func validateCanonicalTikTokURL(rawURL string) (string, string, error) {
	rawURL = strings.TrimSpace(rawURL)
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.User != nil || !strings.EqualFold(parsed.Scheme, "https") || !isAllowedTikTokHost(parsed.Hostname()) || needsTikTokRedirect(parsed) {
		return "", "", fmt.Errorf("TikTok链接无效")
	}
	match := tiktokVideoPattern.FindStringSubmatch(parsed.Path)
	if len(match) < 2 {
		match = tiktokLooseVideoPattern.FindStringSubmatch(parsed.Path)
	}
	if len(match) < 2 {
		return "", "", fmt.Errorf("未识别到TikTok视频ID")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	host := strings.ToLower(parsed.Hostname())
	if host == "m.tiktok.com" || host == "tiktok.com" {
		host = "www.tiktok.com"
	}
	parsed.Host = host
	return parsed.String(), match[1], nil
}

func safeTikTokClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: tiktokTransport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			if req.URL == nil || !strings.EqualFold(req.URL.Scheme, "https") || !isAllowedTikTokHost(req.URL.Hostname()) {
				return fmt.Errorf("redirect target is not allowed")
			}
			return nil
		},
	}
}

func isAllowedTikTokHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	return host == "tiktok.com" || strings.HasSuffix(host, ".tiktok.com")
}

func isTikTokShortHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "vm.tiktok.com" || host == "vt.tiktok.com"
}

func needsTikTokRedirect(value *url.URL) bool {
	if value == nil {
		return false
	}
	if isTikTokShortHost(value.Hostname()) {
		return true
	}
	path := strings.ToLower(strings.TrimSpace(value.Path))
	return strings.HasPrefix(path, "/t/")
}

func isHTTPSURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && strings.EqualFold(parsed.Scheme, "https") && parsed.Hostname() != ""
}
