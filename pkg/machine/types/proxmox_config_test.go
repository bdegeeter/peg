package types

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProxmoxConfigYAML(t *testing.T) {
	var cfg MachineConfig
	err := yaml.Unmarshal([]byte(`
engine: proxmox
cpuType: host
ssh:
  host: pve.example.test
  port: "2222"
proxmox:
  apiURL: https://pve.example.test:8006/api2/json
  node: pve
  username: root@pam
  password: secret
  storage: local-lvm
  isoStorage: local
  bridge: vnet1
  zone: nat
  rngSource: /dev/urandom
  insecureTLS: true
`), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != Proxmox || cfg.CPUType != "host" || cfg.SSH.Host != "pve.example.test" {
		t.Fatalf("unexpected machine config: %#v", cfg)
	}
	if cfg.Proxmox == nil || cfg.Proxmox.Username != "root@pam" || cfg.Proxmox.RNGSource != "/dev/urandom" || !cfg.Proxmox.InsecureTLS {
		t.Fatalf("unexpected Proxmox config: %#v", cfg.Proxmox)
	}
}

func TestProxmoxMachineOptions(t *testing.T) {
	cfg := DefaultMachineConfig()
	err := cfg.Apply(
		ProxmoxEngine,
		WithSSHHost("pve.example.test"),
		WithProxmoxAPIURL("https://pve.example.test:8006/api2/json"),
		WithProxmoxNode("pve"),
		WithProxmoxUsername("root@pam"),
		WithProxmoxPassword("secret"),
		WithProxmoxStorage("local-lvm"),
		WithProxmoxISOStorage("local"),
		WithProxmoxBridge("vnet1"),
		WithProxmoxZone("nat"),
		WithProxmoxRNGSource("/dev/urandom"),
		WithProxmoxInsecureTLS(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != Proxmox || cfg.SSH.Host != "pve.example.test" || cfg.Proxmox == nil {
		t.Fatalf("unexpected machine config: %#v", cfg)
	}
	if cfg.Proxmox.ISOStorage != "local" || cfg.Proxmox.RNGSource != "/dev/urandom" || !cfg.Proxmox.InsecureTLS {
		t.Fatalf("unexpected Proxmox options: %#v", cfg.Proxmox)
	}
}
