package docs

import _ "embed"

// SwaggerJSON is the OpenAPI document served by the API and Scalar UI.
//
//go:embed swagger.json
var SwaggerJSON []byte
