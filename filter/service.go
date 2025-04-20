package filter

import (
	"context"
	"io"
	"os/exec"
	"strings"
)

type ServiceFilter struct {
	cmd *exec.Cmd
}

var _ Filter = (*ServiceFilter)(nil)

func NewServiceFilter(ctx context.Context, serviceId string) *ServiceFilter {
	cmd := exec.CommandContext(ctx, "mirakc-arib", "filter-service", "--sid", serviceId)

	return &ServiceFilter{
		cmd: cmd,
	}
}

func (f *ServiceFilter) Pipe() (io.Writer, io.Reader, error) {
	stdin, err := f.cmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}

	stdout, err := f.cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}

	return stdin, stdout, nil
}

func (f *ServiceFilter) Filter() error {
	if err := f.cmd.Run(); err != nil && !strings.HasPrefix(err.Error(), "signal:") {
		return err
	}
	return nil
}
