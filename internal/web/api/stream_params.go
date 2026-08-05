package api

import apigen "github.com/21S1298001/mahiron/internal/web/api/gen"

func shouldDecode(decode apigen.OptInt) bool {
	value, ok := decode.Get()
	if !ok {
		return true
	}
	return value != 0
}

func shouldAllowCache(allowCache apigen.OptInt) bool {
	value, ok := allowCache.Get()
	if !ok {
		return true
	}
	return value != 0
}
