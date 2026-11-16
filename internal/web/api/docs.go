package api

import (
	"context"
	"embed"

	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"sigs.k8s.io/yaml"
)

//go:embed api.yml
var f embed.FS

func (h *Handler) GetApiDocumentation(ctx context.Context) (apigen.GetApiDocumentationRes, error) {
	res := &apigen.GetApiDocumentationOK{}

	schemaYaml, err := f.ReadFile("api.yml")
	if err != nil {
		return nil, err
	}

	schema, err := yaml.YAMLToJSON(schemaYaml)
	if err != nil {
		return nil, err
	}

	err = res.UnmarshalJSON(schema)
	if err != nil {
		return nil, err
	}

	return res, nil
}
