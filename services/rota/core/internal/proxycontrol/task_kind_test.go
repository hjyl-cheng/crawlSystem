package proxycontrol

import "testing"

func TestChannelRoleAllowsContentEnrichTaskKind(t *testing.T) {
	if !validTaskKind(TaskKindContentEnrich) {
		t.Fatal("content_enrich must be a first-class Task Kind")
	}
	if !roleAllowsTaskKind(RoleChannel, TaskKindContentEnrich) {
		t.Fatal("Channel Role must allow content_enrich")
	}
	if roleAllowsTaskKind(RoleDiscover, TaskKindContentEnrich) {
		t.Fatal("Discover Role must not allow content_enrich")
	}
}
