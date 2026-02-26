package internal

import (
	"fmt"
	"strings"
)

// resolveField performs basic template resolution on value, replacing
// {{.field}} references with values looked up from triggerData, stepOutputs,
// and current (in that priority order).
//
// Supported reference forms:
//
//	{{.field}}                     — look up "field" in triggerData
//	{{.steps.stepName.field}}      — look up stepOutputs["stepName"]["field"]
//	{{.current.field}}             — look up "field" in current
//
// If the placeholder cannot be resolved the original placeholder text is left
// in place so misconfiguration is visible rather than silently swallowed.
func resolveField(value string, triggerData map[string]any, stepOutputs map[string]map[string]any, current map[string]any) string {
	if !strings.Contains(value, "{{") {
		return value
	}

	result := value
	// Iterate until no more replacements can be made (handles multiple refs).
	for strings.Contains(result, "{{") {
		start := strings.Index(result, "{{")
		end := strings.Index(result, "}}")
		if end < start {
			break
		}
		placeholder := result[start : end+2]
		inner := strings.TrimSpace(result[start+2 : end])

		resolved, ok := lookupRef(inner, triggerData, stepOutputs, current)
		if ok {
			result = strings.Replace(result, placeholder, fmt.Sprintf("%v", resolved), 1)
		} else {
			// Leave the unresolvable placeholder and stop to avoid an infinite loop.
			break
		}
	}
	return result
}

// lookupRef resolves a single template reference (the content between {{ and }}).
func lookupRef(ref string, triggerData map[string]any, stepOutputs map[string]map[string]any, current map[string]any) (any, bool) {
	// Strip leading dot.
	ref = strings.TrimPrefix(ref, ".")

	parts := strings.SplitN(ref, ".", 3)

	switch parts[0] {
	case "steps":
		// {{.steps.<stepName>.<field>}}
		if len(parts) < 3 {
			return nil, false
		}
		stepName, field := parts[1], parts[2]
		if stepOutputs == nil {
			return nil, false
		}
		outputs, ok := stepOutputs[stepName]
		if !ok {
			return nil, false
		}
		v, ok := outputs[field]
		return v, ok

	case "current":
		// {{.current.<field>}}
		if len(parts) < 2 {
			return nil, false
		}
		field := strings.Join(parts[1:], ".")
		if current == nil {
			return nil, false
		}
		v, ok := current[field]
		return v, ok

	default:
		// {{.field}} — look up directly in triggerData.
		field := strings.Join(parts, ".")
		if triggerData == nil {
			return nil, false
		}
		v, ok := triggerData[field]
		return v, ok
	}
}
