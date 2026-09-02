package machine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/codingsince1985/checksum"
	proxmoxapi "github.com/luthermonson/go-proxmox"
	"github.com/spectrocloud/peg/pkg/machine/internal/utils"
	"github.com/spectrocloud/peg/pkg/machine/types"
)

// --- ISO transfer helpers ---

const (
	// Timeout for ISO download tasks (generous for multi-GB ISOs)
	isoTransferTimeout = 600 // seconds
)

// isProxmoxStorageRef returns true if the ISO string is a Proxmox storage
// reference like "local:iso/foo.iso" or "ceph:iso/bar.iso".
func isProxmoxStorageRef(iso string) bool {
	parts := strings.SplitN(iso, ":", 2)
	if len(parts) != 2 {
		return false
	}
	return strings.HasPrefix(parts[1], "iso/")
}

// prepareISO handles ISO provisioning for the Proxmox backend.
// It detects the ISO source type and ensures the ISO is available on Proxmox storage.
func (p *Proxmox) prepareISO(ctx context.Context, cfg *types.ProxmoxConfig) error {
	iso := p.machineConfig.ISO
	if iso == "" {
		return nil
	}

	// Already a Proxmox storage reference — use as-is
	if isProxmoxStorageRef(iso) {
		log.Infof("ISO is a Proxmox storage reference, using as-is: %s", iso)
		return nil
	}

	isoStorage := cfg.ISOStorage
	if isoStorage == "" {
		isoStorage = "local"
	}

	// HTTP/HTTPS URL — tell Proxmox to download directly
	if utils.IsValidURL(iso) {
		log.Infof("ISO is a URL, directing Proxmox to download: %s", iso)
		return p.downloadURLToProxmox(ctx, cfg, iso, isoStorage)
	}

	// Local file path — upload directly through the authenticated Proxmox API.
	log.Infof("ISO is a local file, preparing Proxmox upload: %s", iso)
	return p.transferLocalISO(ctx, cfg, iso, isoStorage)
}

// downloadURLToProxmox tells Proxmox to download an ISO directly from a URL.
func (p *Proxmox) downloadURLToProxmox(ctx context.Context, cfg *types.ProxmoxConfig, isoURL, isoStorage string) error {
	parsedURL, err := url.Parse(isoURL)
	if err != nil {
		return fmt.Errorf("invalid ISO URL %q: %w", isoURL, err)
	}
	filename := proxmoxURLISOName(isoURL, parsedURL.Path)

	opts := &proxmoxapi.StorageDownloadURLOptions{
		Content:  "iso",
		Filename: filename,
		Storage:  isoStorage,
		URL:      isoURL,
		Node:     cfg.Node,
	}

	// Use checksum if provided
	if p.machineConfig.ISOChecksum != "" {
		alg, hash, err := parseChecksum(p.machineConfig.ISOChecksum)
		if err != nil {
			return err
		}
		opts.Checksum = hash
		opts.ChecksumAlgorithm = alg
	}

	upid, err := p.node.StorageDownloadURL(ctx, opts)
	if err != nil {
		return fmt.Errorf("StorageDownloadURL failed: %w", err)
	}

	task := proxmoxapi.NewTask(proxmoxapi.UPID(upid), p.client)
	if err := waitForSuccessfulProxmoxTask(ctx, task, isoTransferTimeout, "ISO download"); err != nil {
		return err
	}

	p.machineConfig.ISO = fmt.Sprintf("%s:iso/%s", isoStorage, filename)
	log.Infof("ISO available on Proxmox storage: %s", p.machineConfig.ISO)
	return nil
}

// transferLocalISO streams a local ISO through Proxmox's authenticated upload API.
func (p *Proxmox) transferLocalISO(ctx context.Context, cfg *types.ProxmoxConfig, isoPath, isoStorage string) error {
	// Validate the local file exists
	fi, err := os.Stat(isoPath)
	if err != nil {
		return fmt.Errorf("ISO file not found: %w", err)
	}

	log.Infof("Computing SHA256 checksum for %s...", isoPath)
	sha256sum, err := checksum.SHA256sum(isoPath)
	if err != nil {
		return fmt.Errorf("failed to compute SHA256: %w", err)
	}
	filename := fmt.Sprintf("peg-%s-%s", sha256sum[:12], sanitizeProxmoxName(filepath.Base(isoPath)))

	// Check if ISO already exists on Proxmox with matching size
	exists, err := p.isoExistsOnStorage(ctx, isoStorage, filename, fi.Size())
	if err != nil {
		return fmt.Errorf("failed to check existing ISO: %w", err)
	}
	if exists {
		log.Infof("ISO %q already current on Proxmox storage %q, skipping upload", filename, isoStorage)
		p.machineConfig.ISO = fmt.Sprintf("%s:iso/%s", isoStorage, filename)
		return nil
	}
	log.Infof("Uploading ISO to Proxmox storage %q: %s (%d bytes, %.2f GiB)",
		isoStorage, isoPath, fi.Size(), float64(fi.Size())/(1024*1024*1024))

	stagedISOPath, cleanup, err := stageProxmoxISO(isoPath, filename)
	if err != nil {
		return fmt.Errorf("failed to stage ISO for Proxmox upload: %w", err)
	}
	defer cleanup()

	storage, err := p.node.Storage(ctx, isoStorage)
	if err != nil {
		return fmt.Errorf("failed to get storage %q for ISO upload: %w", isoStorage, err)
	}
	task, err := storage.Upload("iso", stagedISOPath)
	if err != nil {
		return fmt.Errorf("failed to upload ISO through Proxmox API: %w", err)
	}
	if err := waitForSuccessfulProxmoxTask(ctx, task, isoTransferTimeout, "ISO upload"); err != nil {
		return err
	}
	exists, err = p.isoExistsOnStorage(ctx, isoStorage, filename, fi.Size())
	if err != nil {
		return fmt.Errorf("failed to verify uploaded ISO: %w", err)
	}
	if !exists {
		return fmt.Errorf("uploaded ISO %q was not found on storage %q", filename, isoStorage)
	}

	p.machineConfig.ISO = fmt.Sprintf("%s:iso/%s", isoStorage, filename)
	log.Infof("ISO uploaded to Proxmox storage: %s (SHA256: %s)", p.machineConfig.ISO, sha256sum)
	return nil
}

// stageProxmoxISO creates a temporary alias whose basename becomes the
// multipart upload filename. go-proxmox's UploadWithName sends the requested
// name as a second scalar "filename" field, which the Proxmox upload endpoint
// does not accept. Upload derives the remote name from the opened file instead.
func stageProxmoxISO(isoPath, filename string) (string, func(), error) {
	absISOPath, err := filepath.Abs(isoPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to resolve ISO path: %w", err)
	}

	stageDir, err := os.MkdirTemp("", "peg-proxmox-upload-")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temporary upload directory: %w", err)
	}
	cleanup := func() {
		if err := os.RemoveAll(stageDir); err != nil {
			log.Warnf("Failed to clean up temporary Proxmox upload directory %q: %v", stageDir, err)
		}
	}

	stagedISOPath := filepath.Join(stageDir, filename)
	if err := os.Symlink(absISOPath, stagedISOPath); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to create temporary ISO alias: %w", err)
	}

	return stagedISOPath, cleanup, nil
}

func waitForSuccessfulProxmoxTask(ctx context.Context, task *proxmoxapi.Task, timeoutSeconds int, operation string) error {
	if task == nil {
		return fmt.Errorf("%s did not return a Proxmox task", operation)
	}
	successful, completed, err := task.WaitForCompleteStatus(ctx, timeoutSeconds)
	if err != nil {
		return fmt.Errorf("%s task failed: %w", operation, err)
	}
	if !completed {
		return fmt.Errorf("%s task did not complete within %d seconds", operation, timeoutSeconds)
	}
	if !successful {
		exitStatus := task.ExitStatus
		if exitStatus == "" {
			exitStatus = task.Status
		}
		return fmt.Errorf("%s task completed unsuccessfully: %s", operation, exitStatus)
	}
	return nil
}

// isoExistsOnStorage checks whether a Peg-owned, content-addressed ISO already
// exists. A size mismatch is reported rather than deleting shared storage.
func (p *Proxmox) isoExistsOnStorage(ctx context.Context, storageName, filename string, localSize int64) (bool, error) {
	storage, err := p.node.Storage(ctx, storageName)
	if err != nil {
		return false, fmt.Errorf("failed to get storage %q: %w", storageName, err)
	}

	contents, err := storage.GetContent(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to list storage %q: %w", storageName, err)
	}
	return pegISOExists(contents, storageName, filename, localSize)
}

func pegISOExists(contents []*proxmoxapi.StorageContent, storageName, filename string, localSize int64) (bool, error) {
	wantedVolume := fmt.Sprintf("%s:iso/%s", storageName, filename)
	for _, content := range contents {
		if content == nil || content.Volid != wantedVolume {
			continue
		}
		remoteSize := int64(content.Size)
		if remoteSize == localSize {
			return true, nil
		}
		return false, fmt.Errorf("ISO %q exists with unexpected size (local: %d, remote: %d); refusing to delete it", filename, localSize, remoteSize)
	}
	return false, nil
}

// parseChecksum splits a checksum string like "sha256:abc123" into algorithm and hash.
// If no algorithm prefix is present, defaults to "sha256".
func parseChecksum(cs string) (alg, hash string, err error) {
	parts := strings.SplitN(cs, ":", 2)
	if len(parts) == 2 {
		alg, hash = strings.ToLower(parts[0]), parts[1]
	} else {
		alg, hash = "sha256", cs
	}
	if hash == "" {
		return "", "", fmt.Errorf("ISO checksum hash is empty")
	}
	switch alg {
	case "md5", "sha1", "sha224", "sha256", "sha384", "sha512":
		return alg, hash, nil
	default:
		return "", "", fmt.Errorf("unsupported Proxmox ISO checksum algorithm %q", alg)
	}
}

func proxmoxURLISOName(isoURL, urlPath string) string {
	urlHash := sha256.Sum256([]byte(isoURL))
	return fmt.Sprintf("peg-%x-%s", urlHash[:6], sanitizeProxmoxName(filepath.Base(urlPath)))
}

func sanitizeProxmoxName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" || name == "." {
		return "image.iso"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, name)
}
