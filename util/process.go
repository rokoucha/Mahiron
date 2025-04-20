package util

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"syscall"
)

type Process struct {
	args   []string
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func NewProcess(args []string) (*Process, error) {
	cmd := exec.Command(args[0], args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	return &Process{
		args:   args,
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}, nil
}

func (p *Process) Stdin() io.Writer {
	return p.stdin
}

func (p *Process) Stdout() io.Reader {
	return p.stdout
}

func (p *Process) Stderr() io.Reader {
	return p.stderr
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
	defer func() {
		p.stdin.Close()
		p.stdout.Close()
		p.stderr.Close()
	}()

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
