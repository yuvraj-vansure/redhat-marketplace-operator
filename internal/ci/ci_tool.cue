package ci

import (
	"tool/file"
	"tool/http"
	"tool/exec"
	"encoding/yaml"
)

command: genworkflows: task: {
	for w in workflows {
		"\(w.file)": file.Create & {
			filename: w.file
			contents: """
				# Generated by internal/ci/ci_tool.cue; do not edit

				\(yaml.Marshal(w.schema))
				"""
		}
	}
}

// vendorgithubschema is expected to be run within the cuelang.org/go
// cue.mod directory
command: vendorgithubschema: {
	get: http.Get & {
		request: body: ""

		// Tip link: https://github.com/SchemaStore/schemastore/blob/master/src/schemas/json/github-workflow.json
		url: "https://raw.githubusercontent.com/SchemaStore/schemastore/6fe4707b9d1c5d45cfc8d5b6d56968e65d2bdc38/src/schemas/json/github-workflow.json"
	}
	convert: exec.Run & {
		stdin: get.response.body
		cmd:   "go run cuelang.org/go/cmd/cue import -f -p json -l #Workflow: jsonschema: - --outfile pkg/github.com/SchemaStore/schemastore/src/schemas/json/github-workflow.cue"
	}
}
