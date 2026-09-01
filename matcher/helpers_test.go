package matcher

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/spectrocloud/peg/pkg/machine/types"
)

type testMachine struct {
	types.Machine
	config   types.MachineConfig
	stopErr  error
	cleanErr error
}

func (m *testMachine) Config() types.MachineConfig { return m.config }
func (m *testMachine) Stop() error                 { return m.stopErr }
func (m *testMachine) Clean() error                { return m.cleanErr }
func (m *testMachine) Command(string) (string, error) {
	return "ping\n", nil
}

type resettableTestMachine struct {
	*testMachine
	resetCalled bool
}

func (m *resettableTestMachine) HardReset(ctx context.Context) (context.Context, error) {
	m.resetCalled = true
	return ctx, nil
}

var _ = Describe("VM.HardPowerCycle", func() {
	Context("when the backend does not support hard resets", func() {
		It("returns an informative error and the original context", func() {
			vm := VM{machine: &testMachine{}}
			ctx, err := vm.HardPowerCycle(context.Background(), 60)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("hard power cycle is not supported"))
			Expect(ctx).NotTo(BeNil())
		})
	})

	Context("when the backend supports hard resets", func() {
		It("uses the capability and reconnects", func() {
			machine := &resettableTestMachine{testMachine: &testMachine{}}
			vm := VM{machine: machine}
			ctx, err := vm.HardPowerCycle(context.Background(), 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(machine.resetCalled).To(BeTrue())
			Expect(ctx).NotTo(BeNil())
		})
	})
})

var _ = Describe("VM metadata and cleanup", func() {
	It("exposes the configured engine", func() {
		vm := VM{machine: &testMachine{config: types.MachineConfig{Engine: types.Proxmox}}}
		Expect(vm.Engine()).To(Equal(types.Proxmox))
	})

	It("returns both stop and cleanup failures", func() {
		vm := VM{machine: &testMachine{stopErr: errors.New("stop failed"), cleanErr: errors.New("clean failed")}}
		err := vm.Destroy(nil)
		Expect(err).To(MatchError(And(ContainSubstring("stop machine: stop failed"), ContainSubstring("clean machine: clean failed"))))
	})
})
