package util

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"syscall"
)

type Process struct {
	cmd *exec.Cmd
}

type ProcessConfig struct {
	Args []string
}

func NewProcess(config ProcessConfig) (*Process, error) {
	cmd := exec.Command(config.Args[0], config.Args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	return &Process{
		cmd: cmd,
	}, nil
}

func (p *Process) StdinPipe() (io.WriteCloser, error) {
	return p.cmd.StdinPipe()
}

func (p *Process) StdoutPipe() (io.ReadCloser, error) {
	return p.cmd.StdoutPipe()
}

func (p *Process) StderrPipe() (io.ReadCloser, error) {
	return p.cmd.StderrPipe()
}

func (p *Process) Pid() int {
	if p.cmd.Process == nil {
		return -1
	}
	return p.cmd.Process.Pid
}

func (p *Process) Start() error {
	return p.cmd.Start()
}

func (p *Process) Stop(ctx context.Context) error {
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}

	if err := p.cmd.Wait(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || err.Error() == "signal: terminated" {
			return nil
		}
		return err
	}

	<-ctx.Done()
	if err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL); err != nil {
		return err
	}
	return nil
}
