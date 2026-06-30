package feed

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

func (s *Service) deleteFilesBestEffort(paths []string) {
	if len(paths) == 0 {
		return
	}
	for _, raw := range paths {
		path := normalizeStoragePath(raw)
		if path == "" {
			continue
		}
		if err := s.deleteOneStorageObject(path); err != nil {
			s.Warn("删除发现媒体文件失败", zap.String("path", raw), zap.String("storage_path", path), zap.Error(err))
		}
	}
}

func (s *Service) deleteOneStorageObject(path string) error {
	cfg := s.ctx.GetConfig()
	switch cfg.FileService {
	case config.FileServiceMinio:
		return s.deleteMinioObject(path)
	default:
		// SeaweedFS 支持 HTTP DELETE；本地路径也做一次 best-effort 删除。
		if err := s.deleteByHTTP(path); err == nil {
			return nil
		}
		return os.RemoveAll(path)
	}
}

func (s *Service) deleteMinioObject(path string) error {
	cfg := s.ctx.GetConfig().Minio
	u, err := url.Parse(cfg.UploadURL)
	if err != nil {
		return err
	}
	endpoint := u.Host
	if endpoint == "" {
		endpoint = cfg.UploadURL
	}
	useSSL := strings.HasPrefix(u.Scheme, "https")
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}
	bucket, object := splitBucketObject(path)
	if bucket == "" || object == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{})
}

func (s *Service) deleteByHTTP(path string) error {
	fullURL := path
	if !strings.HasPrefix(fullURL, "http://") && !strings.HasPrefix(fullURL, "https://") {
		base := ""
		if s.ctx.GetConfig().Seaweed.URL != "" {
			base = s.ctx.GetConfig().Seaweed.URL
		}
		if base == "" {
			return os.ErrInvalid
		}
		joined, err := url.JoinPath(base, path)
		if err != nil {
			return err
		}
		fullURL = joined
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, fullURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return os.ErrPermission
}

func normalizeStoragePath(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return ""
	}
	if u, err := url.Parse(v); err == nil && u.Scheme != "" && u.Host != "" {
		v = strings.TrimPrefix(u.Path, "/")
	}
	v = strings.TrimPrefix(v, "/")
	v = strings.TrimPrefix(v, "v1/")
	v = strings.TrimPrefix(v, "file/preview/")
	v = strings.TrimPrefix(v, "web/")
	v = strings.TrimPrefix(v, "api/")
	v = strings.TrimPrefix(v, "./")
	v = filepath.Clean(v)
	if v == "." || strings.HasPrefix(v, "../") || v == ".." {
		return ""
	}
	return strings.TrimPrefix(v, "/")
}

func splitBucketObject(path string) (string, string) {
	path = strings.TrimPrefix(path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 1 {
		return "file", parts[0]
	}
	return parts[0], parts[1]
}
