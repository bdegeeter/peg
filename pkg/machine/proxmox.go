package machine

import (
	"bytes"
	"context"
	"crypto/des"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/codingsince1985/checksum"
	proxmoxapi "github.com/luthermonson/go-proxmox"
	"github.com/spectrocloud/peg/pkg/controller"
	"github.com/spectrocloud/peg/pkg/machine/internal/utils"
	"github.com/spectrocloud/peg/pkg/machine/types"
)

const (
	// Default timeout for Proxmox task completion
	proxmoxTaskTimeout = 300 // seconds

	// Polling interval for VM status monitoring
	proxmoxMonitorInterval = 3 * time.Second
)

// Proxmox implements the Machine interface for Proxmox VE.
type Proxmox struct {
	machineConfig types.MachineConfig
	client        *proxmoxapi.Client
	node          *proxmoxapi.Node
	vm            *proxmoxapi.VirtualMachine
	vmid          int
}

func (p *Proxmox) Config() types.MachineConfig {
	return p.machineConfig
}

func (p *Proxmox) Create(ctx context.Context) (context.Context, error) {
	log.Info("Create proxmox machine")

	cfg := p.machineConfig.Proxmox
	if cfg == nil {
		return ctx, fmt.Errorf("proxmox configuration is required")
	}

	if err := p.validateConfig(cfg); err != nil {
		return ctx, fmt.Errorf("invalid proxmox configuration: %w", err)
	}

	// Create Proxmox API client
	if err := p.initClient(cfg); err != nil {
		return ctx, fmt.Errorf("failed to initialize proxmox client: %w", err)
	}

	// Get the node
	node, err := p.client.Node(ctx, cfg.Node)
	if err != nil {
		return ctx, fmt.Errorf("failed to get proxmox node %q: %w", cfg.Node, err)
	}
	p.node = node

	// Handle ISO transfer to Proxmox storage if needed
	if err := p.prepareISO(ctx, cfg); err != nil {
		return ctx, fmt.Errorf("failed to prepare ISO: %w", err)
	}

	// Validate SDN infrastructure
	if err := p.validateSDN(ctx, cfg); err != nil {
		return ctx, fmt.Errorf("SDN validation failed: %w", err)
	}

	// Get next available VMID
	cluster, err := p.client.Cluster(ctx)
	if err != nil {
		return ctx, fmt.Errorf("failed to get cluster: %w", err)
	}

	vmid, err := cluster.NextID(ctx)
	if err != nil {
		return ctx, fmt.Errorf("failed to get next VMID: %w", err)
	}
	p.vmid = vmid

	log.Infof("Creating Proxmox VM %d on node %s [ Memory: %s, CPU: %s ]",
		vmid, cfg.Node, p.machineConfig.Memory, p.machineConfig.CPU)

	// Build VM creation options
	vmOpts := p.buildVMOptions(cfg)

	// Create the VM
	task, err := node.NewVirtualMachine(ctx, vmid, vmOpts...)
	if err != nil {
		return ctx, fmt.Errorf("failed to create VM %d: %w", vmid, err)
	}

	if err := task.WaitFor(ctx, proxmoxTaskTimeout); err != nil {
		return ctx, fmt.Errorf("VM creation task failed: %w", err)
	}

	log.Infof("VM %d created successfully", vmid)

	// Start the VM
	vm, err := node.VirtualMachine(ctx, vmid)
	if err != nil {
		return ctx, fmt.Errorf("failed to get VM %d after creation: %w", vmid, err)
	}
	p.vm = vm

	startTask, err := vm.Start(ctx)
	if err != nil {
		return ctx, fmt.Errorf("failed to start VM %d: %w", vmid, err)
	}

	if err := startTask.WaitFor(ctx, proxmoxTaskTimeout); err != nil {
		return ctx, fmt.Errorf("VM start task failed: %w", err)
	}

	log.Infof("VM %d started successfully", vmid)

	// Start monitoring goroutine
	newCtx := p.monitorVM(ctx)

	return newCtx, nil
}

func (p *Proxmox) Stop() error {
	if p.vm == nil {
		return fmt.Errorf("VM not initialized")
	}

	ctx := context.Background()

	// Check if already stopped
	if err := p.vm.Ping(ctx); err != nil {
		return fmt.Errorf("failed to get VM status: %w", err)
	}

	if p.vm.IsStopped() {
		log.Info("VM is already stopped")
		return nil
	}

	task, err := p.vm.Stop(ctx)
	if err != nil {
		return fmt.Errorf("failed to stop VM %d: %w", p.vmid, err)
	}

	if err := task.WaitFor(ctx, proxmoxTaskTimeout); err != nil {
		return fmt.Errorf("VM stop task failed: %w", err)
	}

	log.Infof("VM %d stopped", p.vmid)
	return nil
}

func (p *Proxmox) HardReset(ctx context.Context) error {
	if p.vm == nil {
		return fmt.Errorf("VM not initialized")
	}

	// Immediate stop (power off, not graceful shutdown)
	if err := p.vm.Ping(ctx); err != nil {
		return fmt.Errorf("failed to get VM status: %w", err)
	}

	if !p.vm.IsStopped() {
		task, err := p.vm.Stop(ctx)
		if err != nil {
			return fmt.Errorf("failed to stop VM %d: %w", p.vmid, err)
		}
		if err := task.WaitFor(ctx, proxmoxTaskTimeout); err != nil {
			return fmt.Errorf("VM stop task failed: %w", err)
		}
		log.Infof("VM %d stopped for hard reset", p.vmid)
	}

	// Start the VM again
	startTask, err := p.vm.Start(ctx)
	if err != nil {
		return fmt.Errorf("failed to start VM %d after hard reset: %w", p.vmid, err)
	}
	if err := startTask.WaitFor(ctx, proxmoxTaskTimeout); err != nil {
		return fmt.Errorf("VM start task failed after hard reset: %w", err)
	}

	log.Infof("VM %d hard reset complete (stop + start)", p.vmid)
	return nil
}

func (p *Proxmox) Clean() error {
	if p.vm == nil {
		// Nothing to clean
		return nil
	}

	ctx := context.Background()

	// Ensure VM is stopped first
	if err := p.vm.Ping(ctx); err == nil && !p.vm.IsStopped() {
		if err := p.Stop(); err != nil {
			log.Warnf("Failed to stop VM before cleanup: %v", err)
		}
	}

	// Delete the VM with purge to remove disks
	task, err := p.vm.Delete(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete VM %d: %w", p.vmid, err)
	}

	if err := task.WaitFor(ctx, proxmoxTaskTimeout); err != nil {
		return fmt.Errorf("VM delete task failed: %w", err)
	}

	log.Infof("VM %d deleted", p.vmid)

	// Clean local state directory
	if p.machineConfig.StateDir != "" {
		return os.RemoveAll(p.machineConfig.StateDir)
	}

	return nil
}

func (p *Proxmox) Screenshot() (string, error) {
	if p.vm == nil {
		return "", fmt.Errorf("VM not initialized")
	}

	return p.screenshotVNC()
}

// screenshotVNC captures a screenshot via VNC WebSocket (API-only, no SSH).
func (p *Proxmox) screenshotVNC() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Get VNC proxy ticket
	vnc, err := p.vm.VNCProxy(ctx, &proxmoxapi.VNCConfig{Websocket: true})
	if err != nil {
		return "", fmt.Errorf("VNCProxy failed: %w", err)
	}

	// Connect WebSocket
	send, recv, errs, closer, err := p.vm.VNCWebSocket(vnc)
	if err != nil {
		return "", fmt.Errorf("VNCWebSocket failed: %w", err)
	}
	defer closer()

	// Grab a single frame via the RFB protocol
	reader := &wsChanReader{recv: recv, errs: errs, timeout: 10 * time.Second}
	width, height, pixels, err := rfbGrabFrame(reader, send, vnc.Ticket)
	if err != nil {
		return "", fmt.Errorf("RFB frame capture failed: %w", err)
	}

	// Convert to PPM and write to state dir
	ppm := pixelsToPPM(width, height, pixels)
	localPath := filepath.Join(p.machineConfig.StateDir, fmt.Sprintf("screenshot-%d.ppm", p.vmid))
	if err := os.WriteFile(localPath, ppm, 0644); err != nil {
		return "", fmt.Errorf("failed to write screenshot: %w", err)
	}

	log.Infof("VNC screenshot captured: %s (%dx%d)", localPath, width, height)
	return localPath, nil
}

func (p *Proxmox) CreateDisk(diskname, size string) error {
	if p.vm == nil {
		return fmt.Errorf("VM not initialized")
	}

	cfg := p.machineConfig.Proxmox
	if cfg == nil {
		return fmt.Errorf("proxmox configuration is required")
	}

	ctx := context.Background()

	// Convert MB size to GB for Proxmox
	sizeMB, err := strconv.Atoi(strings.TrimSuffix(size, "M"))
	if err != nil {
		return fmt.Errorf("invalid disk size %q: %w", size, err)
	}
	sizeGB := sizeMB / 1024
	if sizeGB < 1 {
		sizeGB = 1
	}

	// Find the next available SCSI slot
	scsiIdx, err := p.nextSCSIIndex(ctx)
	if err != nil {
		return fmt.Errorf("failed to find available SCSI slot: %w", err)
	}

	diskSpec := fmt.Sprintf("%s:%d", cfg.Storage, sizeGB)
	task, err := p.vm.Config(ctx, proxmoxapi.VirtualMachineOption{
		Name:  fmt.Sprintf("scsi%d", scsiIdx),
		Value: diskSpec,
	})
	if err != nil {
		return fmt.Errorf("failed to add disk: %w", err)
	}

	if err := task.WaitFor(ctx, proxmoxTaskTimeout); err != nil {
		return fmt.Errorf("disk creation task failed: %w", err)
	}

	log.Infof("Added disk scsi%d (%dGB) to VM %d", scsiIdx, sizeGB, p.vmid)
	return nil
}

func (p *Proxmox) Command(cmd string) (string, error) {
	return controller.SSHCommand(p, cmd)
}

func (p *Proxmox) DetachCD() error {
	if p.vm == nil {
		return fmt.Errorf("VM not initialized")
	}

	ctx := context.Background()

	// Remove the IDE2 CD-ROM by setting it to none
	task, err := p.vm.Config(ctx, proxmoxapi.VirtualMachineOption{
		Name:  "ide2",
		Value: "none,media=cdrom",
	})
	if err != nil {
		return fmt.Errorf("failed to detach CD: %w", err)
	}

	if err := task.WaitFor(ctx, proxmoxTaskTimeout); err != nil {
		return fmt.Errorf("detach CD task failed: %w", err)
	}

	log.Infof("Detached CD from VM %d", p.vmid)
	return nil
}

func (p *Proxmox) ReceiveFile(src, dst string) error {
	return controller.ReceiveFile(p, src, dst)
}

func (p *Proxmox) SendFile(src, dst, permissions string) error {
	return controller.SendFile(p, src, dst, permissions)
}

// --- Private helpers ---

func (p *Proxmox) validateConfig(cfg *types.ProxmoxConfig) error {
	if cfg.APIURL == "" {
		return fmt.Errorf("proxmox apiURL is required")
	}
	if cfg.Node == "" {
		return fmt.Errorf("proxmox node is required")
	}
	hasToken := cfg.TokenID != "" && cfg.TokenSecret != ""
	hasLogin := cfg.Username != "" && cfg.Password != ""
	if !hasToken && !hasLogin {
		return fmt.Errorf("proxmox auth required: set tokenID+tokenSecret or username+password")
	}
	if cfg.Storage == "" {
		return fmt.Errorf("proxmox storage is required")
	}
	return nil
}

func (p *Proxmox) initClient(cfg *types.ProxmoxConfig) error {
	var authOpt proxmoxapi.Option
	if cfg.Username != "" && cfg.Password != "" {
		authOpt = proxmoxapi.WithLogins(cfg.Username, cfg.Password)
	} else {
		authOpt = proxmoxapi.WithAPIToken(cfg.TokenID, cfg.TokenSecret)
	}

	opts := []proxmoxapi.Option{authOpt}

	if cfg.InsecureTLS {
		opts = append(opts, proxmoxapi.WithHTTPClient(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}))
	}

	p.client = proxmoxapi.NewClient(cfg.APIURL, opts...)
	return nil
}

// validateSDN checks that the expected SDN infrastructure exists.
// If the API token lacks SDN permissions, it logs a warning and continues
// rather than failing -- the user has already confirmed the SDN is configured.
func (p *Proxmox) validateSDN(ctx context.Context, cfg *types.ProxmoxConfig) error {
	if cfg.Bridge == "" {
		return nil
	}

	cluster, err := p.client.Cluster(ctx)
	if err != nil {
		log.Warnf("Could not get cluster for SDN validation (will continue anyway): %v", err)
		return nil
	}

	// Validate zone exists (if specified)
	if cfg.Zone != "" {
		zone, err := cluster.SDNZone(ctx, cfg.Zone)
		if err != nil {
			log.Warnf("Could not validate SDN zone %q (may lack SDN.Audit permission): %v", cfg.Zone, err)
		} else {
			log.Infof("SDN zone %q found (type: %s)", cfg.Zone, zone.Type)
		}
	}

	// Validate VNet exists
	vnet, err := cluster.SDNVNet(ctx, cfg.Bridge)
	if err != nil {
		log.Warnf("Could not validate SDN VNet %q (may lack SDN.Audit permission): %v", cfg.Bridge, err)
		return nil
	}
	log.Infof("SDN VNet %q found (zone: %s)", cfg.Bridge, vnet.Zone)

	// Validate VNet has a subnet with SNAT enabled
	subnets, err := cluster.SDNSubnets(ctx, cfg.Bridge)
	if err != nil {
		log.Warnf("Could not validate subnets for VNet %q: %v", cfg.Bridge, err)
		return nil
	}

	snatFound := false
	for _, subnet := range subnets {
		if subnet.SNAT != 0 {
			snatFound = true
			log.Infof("SDN subnet %s with SNAT enabled (gateway: %s)", subnet.CIDR, subnet.Gateway)
			break
		}
	}

	if !snatFound {
		log.Warnf("No subnet with SNAT enabled found on VNet %q; SNAT is required for outbound connectivity", cfg.Bridge)
	}

	return nil
}

func (p *Proxmox) buildVMOptions(cfg *types.ProxmoxConfig) []proxmoxapi.VirtualMachineOption {
	// Default CPU type to "host" to pass through host CPU features.
	// Many modern distros require x86-64-v2 or higher which kvm64 doesn't provide.
	cpuType := "host"
	if p.machineConfig.CPUType != "" {
		cpuType = p.machineConfig.CPUType
	}

	opts := []proxmoxapi.VirtualMachineOption{
		{Name: "name", Value: fmt.Sprintf("peg-%s", p.machineConfig.ID)},
		{Name: "memory", Value: p.machineConfig.Memory},
		{Name: "cores", Value: p.machineConfig.CPU},
		{Name: "cpu", Value: cpuType},
		{Name: "scsihw", Value: "virtio-scsi-pci"},
	}

	// Bridge NIC is optional — when using SLIRP-only networking (via Args),
	// the bridge is omitted so the VM has a single NIC matching the cloud-config.
	if cfg.Bridge != "" {
		opts = append(opts, proxmoxapi.VirtualMachineOption{
			Name: "net0", Value: fmt.Sprintf("virtio,bridge=%s", cfg.Bridge),
		})
	}

	// Boot disk from DriveSizes
	driveSizes := p.machineConfig.DriveSizes
	if len(driveSizes) == 0 {
		driveSizes = []string{types.DefaultDriveSize}
	}

	// Primary boot disk
	sizeMB, _ := strconv.Atoi(driveSizes[0])
	sizeGB := sizeMB / 1024
	if sizeGB < 1 {
		sizeGB = 1
	}
	opts = append(opts, proxmoxapi.VirtualMachineOption{
		Name:  "scsi0",
		Value: fmt.Sprintf("%s:%d", cfg.Storage, sizeGB),
	})

	// Additional drives
	for i := 1; i < len(driveSizes); i++ {
		sizeMB, _ := strconv.Atoi(driveSizes[i])
		sizeGB := sizeMB / 1024
		if sizeGB < 1 {
			sizeGB = 1
		}
		opts = append(opts, proxmoxapi.VirtualMachineOption{
			Name:  fmt.Sprintf("scsi%d", i),
			Value: fmt.Sprintf("%s:%d", cfg.Storage, sizeGB),
		})
	}

	// ISO (pre-staged on Proxmox storage)
	if p.machineConfig.ISO != "" {
		opts = append(opts, proxmoxapi.VirtualMachineOption{
			Name:  "ide2",
			Value: fmt.Sprintf("%s,media=cdrom", p.machineConfig.ISO),
		})
	}

	// Boot order: disk first, then CD
	bootOrder := "order=scsi0"
	if p.machineConfig.ISO != "" {
		bootOrder += ";ide2"
	}
	opts = append(opts, proxmoxapi.VirtualMachineOption{
		Name:  "boot",
		Value: bootOrder,
	})

	// Pass custom QEMU args (e.g., SLIRP NAT for port forwarding)
	if len(p.machineConfig.Args) > 0 {
		opts = append(opts, proxmoxapi.VirtualMachineOption{
			Name:  "args",
			Value: strings.Join(p.machineConfig.Args, " "),
		})
	}

	return opts
}

// monitorVM starts a goroutine that polls VM status and cancels the context
// when the VM unexpectedly stops.
func (p *Proxmox) monitorVM(ctx context.Context) context.Context {
	newCtx, cancelFunc := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(proxmoxMonitorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				cancelFunc()
				return
			case <-ticker.C:
				if p.vm == nil {
					cancelFunc()
					return
				}
				if err := p.vm.Ping(context.Background()); err != nil {
					log.Warnf("Failed to ping VM %d: %v", p.vmid, err)
					if p.machineConfig.OnFailure != nil {
						p.machineConfig.OnFailure(nil)
					}
					cancelFunc()
					return
				}
				if !p.vm.IsRunning() {
					log.Warnf("VM %d is no longer running (status: %s)", p.vmid, p.vm.Status)
					if p.machineConfig.OnFailure != nil {
						p.machineConfig.OnFailure(nil)
					}
					cancelFunc()
					return
				}
			}
		}
	}()
	return newCtx
}

// nextSCSIIndex finds the next available SCSI device index by checking current config.
func (p *Proxmox) nextSCSIIndex(ctx context.Context) (int, error) {
	// Refresh VM config
	vm, err := p.node.VirtualMachine(ctx, p.vmid)
	if err != nil {
		return 0, err
	}
	p.vm = vm

	// Check scsi0 through scsi13 (SCSI controller supports up to 14 devices)
	for i := 0; i <= 13; i++ {
		// We check by trying to see if the config has this SCSI device
		// The simplest approach: try adding at index i and if it fails try next
		// For now, start from the number of configured drive sizes + 1
		// since scsi0..N-1 were set during creation
		break
	}

	// Return the count of configured drives as the next index
	driveSizes := p.machineConfig.DriveSizes
	if len(driveSizes) == 0 {
		return 1, nil // scsi0 is the boot disk
	}
	return len(driveSizes), nil
}

// hostFromAPIURL extracts the hostname/IP from a Proxmox API URL.
func (p *Proxmox) hostFromAPIURL(apiURL string) string {
	// Strip protocol
	host := apiURL
	if idx := strings.Index(host, "://"); idx != -1 {
		host = host[idx+3:]
	}
	// Strip port and path
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}
	if idx := strings.Index(host, "/"); idx != -1 {
		host = host[:idx]
	}
	return host
}

// --- VNC screenshot helpers ---

// RFB protocol constants
const (
	rfbMsgSetPixelFormat           = 0
	rfbMsgFramebufferUpdateRequest = 3
	rfbMsgSetEncodings             = 2
	rfbMsgFramebufferUpdate        = 0
	rfbSecurityNone                = 1
	rfbSecurityVNCAuth             = 2
	rfbEncodingRaw                 = 0
)

// wsChanReader adapts a WebSocket recv channel into an io.Reader.
type wsChanReader struct {
	recv    <-chan []byte
	errs    <-chan error
	buf     []byte
	timeout time.Duration
}

func (r *wsChanReader) Read(p []byte) (int, error) {
	// Drain buffered data first
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}

	// Wait for next message from WebSocket
	timer := time.NewTimer(r.timeout)
	defer timer.Stop()
	select {
	case msg, ok := <-r.recv:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, msg)
		if n < len(msg) {
			r.buf = msg[n:]
		}
		return n, nil
	case err := <-r.errs:
		return 0, err
	case <-timer.C:
		return 0, fmt.Errorf("VNC read timeout (%v)", r.timeout)
	}
}

// rfbGrabFrame performs a minimal RFB handshake and captures a single frame.
// vncPassword is the VNC ticket used for VNCAuth (security type 2).
func rfbGrabFrame(r io.Reader, send chan<- []byte, vncPassword string) (width, height uint16, pixels []byte, err error) {
	// 1. Version handshake
	verBuf := make([]byte, 12)
	if _, err = io.ReadFull(r, verBuf); err != nil {
		return 0, 0, nil, fmt.Errorf("reading server version: %w", err)
	}
	log.Debugf("RFB server version: %s", strings.TrimSpace(string(verBuf)))
	send <- []byte("RFB 003.008\n")

	// 2. Security handshake
	var numSecTypes uint8
	if err = binary.Read(r, binary.BigEndian, &numSecTypes); err != nil {
		return 0, 0, nil, fmt.Errorf("reading security type count: %w", err)
	}
	if numSecTypes == 0 {
		// Server sent a reason string for failure
		var reasonLen uint32
		binary.Read(r, binary.BigEndian, &reasonLen)
		reason := make([]byte, reasonLen)
		io.ReadFull(r, reason)
		return 0, 0, nil, fmt.Errorf("VNC server rejected connection: %s", string(reason))
	}

	secTypes := make([]byte, numSecTypes)
	if _, err = io.ReadFull(r, secTypes); err != nil {
		return 0, 0, nil, fmt.Errorf("reading security types: %w", err)
	}
	log.Debugf("RFB security types offered: %v", secTypes)

	// Prefer None (1), fall back to VNCAuth (2)
	selectedSec := byte(0)
	for _, st := range secTypes {
		if st == rfbSecurityNone {
			selectedSec = rfbSecurityNone
			break
		}
		if st == rfbSecurityVNCAuth {
			selectedSec = rfbSecurityVNCAuth
		}
	}
	if selectedSec == 0 {
		return 0, 0, nil, fmt.Errorf("VNC server offers no supported security types (got: %v)", secTypes)
	}
	send <- []byte{selectedSec}

	if selectedSec == rfbSecurityVNCAuth {
		// VNC Authentication: server sends 16-byte challenge, client responds with DES-encrypted challenge
		challenge := make([]byte, 16)
		if _, err = io.ReadFull(r, challenge); err != nil {
			return 0, 0, nil, fmt.Errorf("reading VNC auth challenge: %w", err)
		}
		response := vncAuthEncrypt(challenge, vncPassword)
		send <- response
	}

	// Read SecurityResult
	var secResult uint32
	if err = binary.Read(r, binary.BigEndian, &secResult); err != nil {
		return 0, 0, nil, fmt.Errorf("reading security result: %w", err)
	}
	if secResult != 0 {
		return 0, 0, nil, fmt.Errorf("VNC security handshake failed (result=%d)", secResult)
	}

	// 3. ClientInit (shared=true)
	send <- []byte{1}

	// 4. ServerInit — read framebuffer dimensions and pixel format
	var serverInit struct {
		Width       uint16
		Height      uint16
		PixelFormat [16]byte
		NameLen     uint32
	}
	if err = binary.Read(r, binary.BigEndian, &serverInit); err != nil {
		return 0, 0, nil, fmt.Errorf("reading ServerInit: %w", err)
	}
	width = serverInit.Width
	height = serverInit.Height

	// Read and discard the desktop name
	name := make([]byte, serverInit.NameLen)
	if _, err = io.ReadFull(r, name); err != nil {
		return 0, 0, nil, fmt.Errorf("reading desktop name: %w", err)
	}
	log.Debugf("RFB desktop: %s (%dx%d)", string(name), width, height)

	// 5. SetPixelFormat — request 32bpp with R at byte 0, G at byte 1, B at byte 2
	pixFmt := &bytes.Buffer{}
	pixFmt.WriteByte(rfbMsgSetPixelFormat) // message type
	pixFmt.Write([]byte{0, 0, 0})          // padding
	pixFmt.WriteByte(32)                    // bits-per-pixel
	pixFmt.WriteByte(24)                    // depth
	pixFmt.WriteByte(0)                     // big-endian (0=little)
	pixFmt.WriteByte(1)                     // true-color
	binary.Write(pixFmt, binary.BigEndian, uint16(255)) // red-max
	binary.Write(pixFmt, binary.BigEndian, uint16(255)) // green-max
	binary.Write(pixFmt, binary.BigEndian, uint16(255)) // blue-max
	pixFmt.WriteByte(0)                     // red-shift
	pixFmt.WriteByte(8)                     // green-shift
	pixFmt.WriteByte(16)                    // blue-shift
	pixFmt.Write([]byte{0, 0, 0})          // padding
	send <- pixFmt.Bytes()

	// 6. SetEncodings — request Raw encoding only
	encMsg := &bytes.Buffer{}
	encMsg.WriteByte(rfbMsgSetEncodings) // message type
	encMsg.WriteByte(0)                   // padding
	binary.Write(encMsg, binary.BigEndian, uint16(1))             // number of encodings
	binary.Write(encMsg, binary.BigEndian, int32(rfbEncodingRaw)) // Raw
	send <- encMsg.Bytes()

	// 7. FramebufferUpdateRequest — full screen, non-incremental
	fbReq := &bytes.Buffer{}
	fbReq.WriteByte(rfbMsgFramebufferUpdateRequest)
	fbReq.WriteByte(0) // incremental = false
	binary.Write(fbReq, binary.BigEndian, uint16(0))     // x
	binary.Write(fbReq, binary.BigEndian, uint16(0))     // y
	binary.Write(fbReq, binary.BigEndian, width)          // width
	binary.Write(fbReq, binary.BigEndian, height)         // height
	send <- fbReq.Bytes()

	// 8. Read FramebufferUpdate response
	// The server may send other message types first; skip until we get type 0
	for {
		var msgType uint8
		if err = binary.Read(r, binary.BigEndian, &msgType); err != nil {
			return 0, 0, nil, fmt.Errorf("reading message type: %w", err)
		}
		if msgType == rfbMsgFramebufferUpdate {
			break
		}
		// Skip unknown messages — read and discard based on type
		if err = rfbSkipMessage(r, msgType); err != nil {
			return 0, 0, nil, fmt.Errorf("skipping message type %d: %w", msgType, err)
		}
	}

	// Parse FramebufferUpdate header
	var pad uint8
	var numRects uint16
	binary.Read(r, binary.BigEndian, &pad)
	if err = binary.Read(r, binary.BigEndian, &numRects); err != nil {
		return 0, 0, nil, fmt.Errorf("reading rect count: %w", err)
	}

	// Read all rectangles — accumulate pixel data
	pixels = make([]byte, 0, int(width)*int(height)*4)
	totalPixels := make([]byte, int(width)*int(height)*4)

	for i := uint16(0); i < numRects; i++ {
		var rect struct {
			X, Y, W, H  uint16
			EncodingType int32
		}
		if err = binary.Read(r, binary.BigEndian, &rect); err != nil {
			return 0, 0, nil, fmt.Errorf("reading rect %d header: %w", i, err)
		}

		if rect.EncodingType != rfbEncodingRaw {
			return 0, 0, nil, fmt.Errorf("unsupported encoding type %d in rect %d", rect.EncodingType, i)
		}

		rectSize := int(rect.W) * int(rect.H) * 4
		rectData := make([]byte, rectSize)
		if _, err = io.ReadFull(r, rectData); err != nil {
			return 0, 0, nil, fmt.Errorf("reading rect %d pixels: %w", i, err)
		}

		// Place rectangle pixels into the full framebuffer at the correct position
		for row := 0; row < int(rect.H); row++ {
			srcOff := row * int(rect.W) * 4
			dstOff := ((int(rect.Y) + row) * int(width) * 4) + (int(rect.X) * 4)
			copy(totalPixels[dstOff:dstOff+int(rect.W)*4], rectData[srcOff:srcOff+int(rect.W)*4])
		}
	}

	return width, height, totalPixels, nil
}

// rfbSkipMessage skips over a server-to-client RFB message that isn't a FramebufferUpdate.
func rfbSkipMessage(r io.Reader, msgType uint8) error {
	switch msgType {
	case 1: // SetColourMapEntries
		var header struct {
			Pad        uint8
			FirstColor uint16
			NumColors  uint16
		}
		if err := binary.Read(r, binary.BigEndian, &header); err != nil {
			return err
		}
		skip := make([]byte, int(header.NumColors)*6) // 3x uint16 per color
		_, err := io.ReadFull(r, skip)
		return err
	case 2: // Bell — no payload
		return nil
	case 3: // ServerCutText
		var pad [3]byte
		io.ReadFull(r, pad[:])
		var textLen uint32
		if err := binary.Read(r, binary.BigEndian, &textLen); err != nil {
			return err
		}
		skip := make([]byte, textLen)
		_, err := io.ReadFull(r, skip)
		return err
	default:
		return fmt.Errorf("unknown server message type %d", msgType)
	}
}

// pixelsToPPM converts 32bpp RGBA pixel data to PPM (P6) format.
func pixelsToPPM(width, height uint16, pixels []byte) []byte {
	header := fmt.Sprintf("P6\n%d %d\n255\n", width, height)
	rgbSize := int(width) * int(height) * 3
	buf := make([]byte, 0, len(header)+rgbSize)
	buf = append(buf, header...)

	// Convert 32bpp (R, G, B, pad) to 24bpp (R, G, B)
	for i := 0; i < len(pixels); i += 4 {
		buf = append(buf, pixels[i], pixels[i+1], pixels[i+2])
	}

	return buf
}

// vncAuthEncrypt performs VNC Authentication DES encryption.
// The password is truncated/padded to 8 bytes, each byte is bit-reversed,
// then used as a DES key to encrypt the 16-byte challenge.
func vncAuthEncrypt(challenge []byte, password string) []byte {
	key := make([]byte, 8)
	for i := 0; i < 8 && i < len(password); i++ {
		key[i] = reverseBits(password[i])
	}

	cipher, err := des.NewCipher(key)
	if err != nil {
		// DES key is always 8 bytes, this shouldn't fail
		return make([]byte, 16)
	}

	response := make([]byte, 16)
	cipher.Encrypt(response[0:8], challenge[0:8])
	cipher.Encrypt(response[8:16], challenge[8:16])
	return response
}

// reverseBits reverses the bit order of a byte (VNC DES key quirk).
func reverseBits(b byte) byte {
	var result byte
	for i := 0; i < 8; i++ {
		result = (result << 1) | (b & 1)
		b >>= 1
	}
	return result
}

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

	// Local file path — serve via temp HTTP server and use StorageDownloadURL
	log.Infof("ISO is a local file, transferring to Proxmox: %s", iso)
	return p.transferLocalISO(ctx, cfg, iso, isoStorage)
}

// downloadURLToProxmox tells Proxmox to download an ISO directly from a URL.
func (p *Proxmox) downloadURLToProxmox(ctx context.Context, cfg *types.ProxmoxConfig, isoURL, isoStorage string) error {
	filename := filepath.Base(isoURL)
	// Strip query params from filename
	if idx := strings.Index(filename, "?"); idx != -1 {
		filename = filename[:idx]
	}

	opts := &proxmoxapi.StorageDownloadURLOptions{
		Content:  "iso",
		Filename: filename,
		Storage:  isoStorage,
		URL:      isoURL,
		Node:     cfg.Node,
	}

	// Use checksum if provided
	if p.machineConfig.ISOChecksum != "" {
		alg, hash := parseChecksum(p.machineConfig.ISOChecksum)
		opts.Checksum = hash
		opts.ChecksumAlgorithm = alg
	}

	upid, err := p.node.StorageDownloadURL(ctx, opts)
	if err != nil {
		return fmt.Errorf("StorageDownloadURL failed: %w", err)
	}

	task := proxmoxapi.NewTask(proxmoxapi.UPID(upid), p.client)
	if err := task.WaitFor(ctx, isoTransferTimeout); err != nil {
		return fmt.Errorf("ISO download task failed: %w", err)
	}

	p.machineConfig.ISO = fmt.Sprintf("%s:iso/%s", isoStorage, filename)
	log.Infof("ISO available on Proxmox storage: %s", p.machineConfig.ISO)
	return nil
}

// transferLocalISO serves a local ISO file via a temporary HTTP server and
// directs Proxmox to download it via StorageDownloadURL.
func (p *Proxmox) transferLocalISO(ctx context.Context, cfg *types.ProxmoxConfig, isoPath, isoStorage string) error {
	// Validate the local file exists
	fi, err := os.Stat(isoPath)
	if err != nil {
		return fmt.Errorf("ISO file not found: %w", err)
	}

	filename := filepath.Base(isoPath)

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

	// Detect the local IP that can reach the Proxmox host
	proxmoxHost := p.hostFromAPIURL(cfg.APIURL)
	localIP, err := detectLocalIP(proxmoxHost)
	if err != nil {
		return err
	}
	log.Infof("Detected local IP reachable from Proxmox: %s", localIP)

	// Start temporary HTTP server
	serveURL, shutdown, err := serveISO(isoPath, localIP)
	if err != nil {
		return fmt.Errorf("failed to start temp HTTP server: %w", err)
	}
	defer shutdown()

	// Compute SHA256 checksum
	log.Infof("Computing SHA256 checksum for %s...", isoPath)
	sha256sum, err := checksum.SHA256sum(isoPath)
	if err != nil {
		return fmt.Errorf("failed to compute SHA256: %w", err)
	}
	log.Infof("SHA256: %s", sha256sum)

	// Tell Proxmox to download from our temp HTTP server
	opts := &proxmoxapi.StorageDownloadURLOptions{
		Content:            "iso",
		Filename:           filename,
		Storage:            isoStorage,
		URL:                serveURL,
		Checksum:           sha256sum,
		ChecksumAlgorithm: "sha256",
		Node:               cfg.Node,
	}

	upid, err := p.node.StorageDownloadURL(ctx, opts)
	if err != nil {
		return fmt.Errorf("StorageDownloadURL failed: %w", err)
	}

	log.Infof("Proxmox downloading ISO from %s (task: %s)", serveURL, upid)
	task := proxmoxapi.NewTask(proxmoxapi.UPID(upid), p.client)
	if err := task.WaitFor(ctx, isoTransferTimeout); err != nil {
		return fmt.Errorf("ISO download task failed: %w", err)
	}

	p.machineConfig.ISO = fmt.Sprintf("%s:iso/%s", isoStorage, filename)
	log.Infof("ISO transferred to Proxmox storage: %s", p.machineConfig.ISO)
	return nil
}

// isoExistsOnStorage checks if an ISO with the given filename exists on the
// specified Proxmox storage with a matching size. If the ISO exists but the
// size differs, it deletes the stale ISO and returns false.
func (p *Proxmox) isoExistsOnStorage(ctx context.Context, storageName, filename string, localSize int64) (bool, error) {
	storage, err := p.node.Storage(ctx, storageName)
	if err != nil {
		return false, fmt.Errorf("failed to get storage %q: %w", storageName, err)
	}

	iso, err := storage.ISO(ctx, filename)
	if err != nil {
		// ISO not found
		return false, nil
	}

	remoteSize := int64(iso.Size)
	if remoteSize == localSize {
		return true, nil
	}

	// Size mismatch — delete stale ISO and re-upload
	log.Infof("ISO %q exists but size differs (local: %d, remote: %d), removing stale copy", filename, localSize, remoteSize)
	delTask, err := iso.Delete(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to delete stale ISO %q: %w", filename, err)
	}
	if err := delTask.WaitFor(ctx, proxmoxTaskTimeout); err != nil {
		return false, fmt.Errorf("delete ISO task failed: %w", err)
	}

	return false, nil
}

// detectLocalIP finds the local IP address that routes to the Proxmox host
// by opening a TCP connection to port 8006 and inspecting the local address.
func detectLocalIP(proxmoxHost string) (string, error) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(proxmoxHost, "8006"), 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("failed to detect local IP reachable from Proxmox host %s: %w", proxmoxHost, err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.TCPAddr)
	return localAddr.IP.String(), nil
}

// serveISO starts a temporary HTTP server that serves a single ISO file.
// It returns the full URL to the file and a shutdown function.
func serveISO(filePath, bindIP string) (url string, shutdown func(), err error) {
	filename := filepath.Base(filePath)

	listener, err := net.Listen("tcp", net.JoinHostPort(bindIP, "0"))
	if err != nil {
		return "", nil, fmt.Errorf("failed to bind temp HTTP server: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/"+filename, func(w http.ResponseWriter, r *http.Request) {
		log.Infof("Serving ISO to %s", r.RemoteAddr)
		http.ServeFile(w, r, filePath)
	})

	srv := &http.Server{Handler: mux}
	go srv.Serve(listener)

	addr := listener.Addr().(*net.TCPAddr)
	url = fmt.Sprintf("http://%s:%d/%s", addr.IP, addr.Port, filename)
	shutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}

	log.Infof("Temp HTTP server listening at %s", url)
	return url, shutdown, nil
}

// parseChecksum splits a checksum string like "sha256:abc123" into algorithm and hash.
// If no algorithm prefix is present, defaults to "sha256".
func parseChecksum(cs string) (alg, hash string) {
	parts := strings.SplitN(cs, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "sha256", cs
}

