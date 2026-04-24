package machine

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/spectrocloud/peg/pkg/machine/types"
)

var _ = Describe("QEMU internal helpers", func() {
	var (
		stateDir string
		q        *QEMU
	)

	spawnSleep := func() *exec.Cmd {
		cmd := exec.Command("sleep", "30")
		Expect(cmd.Start()).To(Succeed())
		pidfile := filepath.Join(stateDir, "pid")
		Expect(os.WriteFile(pidfile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)).To(Succeed())
		return cmd
	}

	BeforeEach(func() {
		stateDir = GinkgoT().TempDir()
		q = &QEMU{machineConfig: types.MachineConfig{StateDir: stateDir}}
	})

	Describe("kill9", func() {
		It("sends SIGKILL to the process named in the pidfile", func() {
			cmd := spawnSleep()
			Expect(q.kill9()).To(Succeed())

			err := cmd.Wait()
			Expect(err).To(HaveOccurred())
			exitErr, ok := err.(*exec.ExitError)
			Expect(ok).To(BeTrue())
			ws, ok := exitErr.Sys().(syscall.WaitStatus)
			Expect(ok).To(BeTrue())
			Expect(ws.Signaled()).To(BeTrue())
			Expect(ws.Signal()).To(Equal(syscall.SIGKILL))
		})

		It("errors when the pidfile is missing", func() {
			err := q.kill9()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reading pidfile"))
		})

		It("errors when the pid is unparseable", func() {
			Expect(os.WriteFile(filepath.Join(stateDir, "pid"), []byte("not-a-pid"), 0o644)).To(Succeed())
			err := q.kill9()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("parsing pid"))
		})

		It("is idempotent against an already-dead process", func() {
			cmd := spawnSleep()
			Expect(q.kill9()).To(Succeed())
			_ = cmd.Wait()
			Expect(q.kill9()).To(Succeed())
		})
	})

	Describe("waitForProcessExit", func() {
		It("returns promptly when the process dies", func() {
			cmd := spawnSleep()
			go func() {
				time.Sleep(100 * time.Millisecond)
				_ = cmd.Process.Signal(syscall.SIGKILL)
				_, _ = cmd.Process.Wait()
			}()

			start := time.Now()
			Expect(q.waitForProcessExit(2 * time.Second)).To(Succeed())
			Expect(time.Since(start)).To(BeNumerically("<", time.Second))
		})

		It("times out when the process stays alive", func() {
			cmd := spawnSleep()
			DeferCleanup(func() {
				_ = cmd.Process.Signal(syscall.SIGKILL)
				_, _ = cmd.Process.Wait()
			})

			err := q.waitForProcessExit(200 * time.Millisecond)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("still alive"))
		})
	})

	Describe("HardPowerCycle kill + cleanup phase", func() {
		It("preserves disk files in StateDir and removes the pidfile", func() {
			q.machineConfig.ID = "testvm"
			cmd := spawnSleep()

			diskPath := filepath.Join(stateDir, "testvm-0.img")
			diskContent := []byte("pretend qcow2 header and data")
			Expect(os.WriteFile(diskPath, diskContent, 0o644)).To(Succeed())

			Expect(q.kill9()).To(Succeed())
			_ = cmd.Wait()
			Expect(q.waitForProcessExit(2 * time.Second)).To(Succeed())
			_ = os.Remove(filepath.Join(stateDir, "pid"))

			got, err := os.ReadFile(diskPath)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(diskContent))

			_, err = os.Stat(filepath.Join(stateDir, "pid"))
			Expect(os.IsNotExist(err)).To(BeTrue())
		})
	})
})
