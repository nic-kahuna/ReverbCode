//go:build !darwin || !cgo

package custody

import "os/exec"

func ProcessCapability() error                            { return ErrUnsupported }
func ObserveProcess(int) (ProcessIdentity, error)         { return ProcessIdentity{}, ErrUnsupported }
func SignalProcess(ProcessIdentity, bool) error           { return ErrUnsupported }
func ProcessInventory() ([]ProcessIdentity, []int, error) { return nil, nil, ErrUnsupported }

func ProcessGone(ProcessIdentity) (bool, error) { return false, ErrUnsupported }

func preparationCommand(string, ...string) (*exec.Cmd, error) { return nil, ErrUnsupported }

func preservationSpace(string, int64) error { return ErrUnsupported }
