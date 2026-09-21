# Drill-only OPA-policy för E7-drillen (deployments/docker-compose.yaml).
#
# Får ALDRIG bakas in i imagen (Dockerfile använder assets/config/authz.rego)
# eller användas utanför den isolerade drill-miljön: den accepterar en statisk
# token utan kryptovalidering och ger fulla scope i tenanten "drill".
# Monteras read-only via compose + POLICIES_FILE-override.
package example.authz

default allow := false

allow = response if {
	input.token == "drill-token"

	response := {
		"tenants": ["drill"],
	}
}
