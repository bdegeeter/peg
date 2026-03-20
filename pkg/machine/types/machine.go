package types

import "context"

type Machine interface {
	Config() MachineConfig
	Create(ctx context.Context) (context.Context, error)
	Stop() error
	// HardReset performs an abrupt power cycle via the hypervisor (not a graceful reboot).
	// For QEMU this sends system_reset via the monitor socket.
	// For Proxmox this does an immediate Stop + Start via the API.
	HardReset(ctx context.Context) error
	Clean() error
	Screenshot() (string, error)
	CreateDisk(diskname, size string) error
	Command(cmd string) (string, error)
	DetachCD() error
	ReceiveFile(src, dst string) error
	SendFile(src, dst, permissions string) error
}
