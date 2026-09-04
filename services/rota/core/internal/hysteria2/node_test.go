package hysteria2

import (
	"strings"
	"testing"
)

const testPin = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestParseCanonicalizesSupportedURI(t *testing.T) {
	raw := "hysteria2://paid%3Acredential@Node.Example.COM:8443/?mport=20003-20000&sni=cdn.example.com&insecure=false&pinSHA256=AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA-AA#Paid%20Node"
	node, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if node.Protocol() != "hysteria2" || node.Address() != "node.example.com:8443" || node.Name() != "Paid Node" {
		t.Fatalf("node = protocol=%q address=%q name=%q", node.Protocol(), node.Address(), node.Name())
	}
	if node.portSet != "20000-20003" {
		t.Fatalf("port set = %q", node.portSet)
	}
	if node.pinSHA256 != testPin {
		t.Fatalf("pin was not normalized")
	}
	if !strings.HasPrefix(node.Credential(), "hysteria2://") || strings.Contains(node.Credential(), "#") {
		t.Fatalf("credential was not canonicalized")
	}
	if len(node.Identity()) != 64 {
		t.Fatalf("identity = %q", node.Identity())
	}
}

func TestParseTreatsHY2AsCanonicalAlias(t *testing.T) {
	long, err := Parse("hysteria2://secret@example.com:443/?sni=cdn.example.com")
	if err != nil {
		t.Fatal(err)
	}
	short, err := Parse("hy2://secret@example.com:443?sni=cdn.example.com#Alias")
	if err != nil {
		t.Fatal(err)
	}
	if long.Credential() != short.Credential() || long.Identity() != short.Identity() {
		t.Fatal("hy2 alias changed canonical node identity")
	}
}

func TestParseSupportsOfficialAuthorityPortHopping(t *testing.T) {
	node, err := Parse("hysteria2://secret@better.call:7000-10000,20000/")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if node.Address() != "better.call:7000" || node.portSet != "7000-10000,20000" {
		t.Fatalf("endpoint = %q port set = %q", node.Address(), node.portSet)
	}
	reparsed, err := Parse(node.Credential())
	if err != nil {
		t.Fatalf("parse canonical credential: %v", err)
	}
	if reparsed.Identity() != node.Identity() || reparsed.portSet != node.portSet {
		t.Fatal("canonical credential changed authority port hopping identity")
	}
}

func TestParseAcceptsEquivalentPortHoppingAliases(t *testing.T) {
	node, err := Parse("hysteria2://secret@example.com:443/?mport=20003-20000&ports=20000-20003")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if node.portSet != "20000-20003" {
		t.Fatalf("port set = %q", node.portSet)
	}
}

func TestParseSupportsOfficialObfuscationParameters(t *testing.T) {
	node, err := Parse("hysteria2://secret@example.com:443/?obfs=salamander&obfs-password=obfs-secret&sni=cdn.example.com&insecure=1")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if node.obfsType != "salamander" || node.obfsPassword != "obfs-secret" || !node.insecure {
		t.Fatalf("obfs/TLS settings were not retained")
	}
}

func TestParseRejectsInvalidURIsWithoutLeakingSecrets(t *testing.T) {
	tests := []string{
		"hysteria2://top-secret@example.com:443/?mport=invalid",
		"hysteria2://top-secret@example.com:20000-20010/?mport=30000-30010",
		"hysteria2://top-secret@example.com:443/?pinSHA256=secret-pin",
		"hysteria2://top-secret@example.com:443/?obfs=salamander&obfs-password=",
		"hysteria2://top-secret@example.com:443/?obfs=unknown&obfs-password=obfs-secret",
	}
	for _, raw := range tests {
		_, err := Parse(raw)
		if err == nil {
			t.Fatalf("Parse unexpectedly accepted invalid URI")
		}
		for _, secret := range []string{"top-secret", "secret-pin", "obfs-secret", raw} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error leaked secret: %v", err)
			}
		}
	}
}
