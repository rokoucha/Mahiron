package runtimecontext

import "context"

type jobKey struct{}

type JobInfo struct {
	ID   string
	Key  string
	Name string
}

func WithJob(ctx context.Context, info JobInfo) context.Context {
	return context.WithValue(ctx, jobKey{}, info)
}

func JobFromContext(ctx context.Context) (JobInfo, bool) {
	info, ok := ctx.Value(jobKey{}).(JobInfo)
	return info, ok
}
