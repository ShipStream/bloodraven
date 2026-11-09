package controller

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

type apiCRDFile struct {
	Spec struct {
		Names struct {
			Kind       string   `json:"kind"`
			ShortNames []string `json:"shortNames"`
			Categories []string `json:"categories"`
		} `json:"names"`
		Versions []struct {
			Name    string `json:"name"`
			Columns []struct {
				Name     string `json:"name"`
				JSONPath string `json:"jsonPath"`
			} `json:"additionalPrinterColumns"`
			Schema struct {
				OpenAPIV3Schema apiCRDSchema `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

func loadAPICRDFiles(t *testing.T) map[string]apiCRDFile {
	t.Helper()
	out := map[string]apiCRDFile{}
	for _, dir := range []string{"../../config/crd/bases", "../../charts/bloodraven/crds"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
		if err != nil || len(files) != 5 {
			t.Fatalf("%s: want 5 CRDs, got %d (%v)", dir, len(files), err)
		}
		for _, f := range files {
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			var crd apiCRDFile
			if err := yaml.Unmarshal(b, &crd); err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			out[f] = crd
		}
	}
	return out
}

// A-93: every kind has short names and the bloodraven and mysql
// categories. No kind takes the generic backup category: kubectl expands a
// category before it resolves a resource name, so `kubectl get backup` and
// `kubectl delete backup --all` would act on MysqlBackups instead of another
// operator's backups resource (Velero, CloudNativePG).
func TestKindShortNamesAndCategories(t *testing.T) {
	want := map[string]struct{ short, categories string }{
		"MysqlFailoverGroup":      {"mfg,mfgs", "bloodraven,mysql"},
		"MysqlDatabase":           {"mydb,mydbs", "bloodraven,mysql"},
		"MysqlBackup":             {"mbk,mbks", "bloodraven,mysql"},
		"MysqlBackupVerification": {"mbv,mbvs", "bloodraven,mysql"},
		"MysqlStandbyCluster":     {"msc,mscs", "bloodraven,mysql,dr"},
	}
	for f, crd := range loadAPICRDFiles(t) {
		n := crd.Spec.Names
		w, ok := want[n.Kind]
		if !ok {
			t.Errorf("%s: unexpected kind %s", f, n.Kind)
			continue
		}
		if got := strings.Join(n.ShortNames, ","); got != w.short {
			t.Errorf("%s: shortNames %s, want %s", f, got, w.short)
		}
		if got := strings.Join(n.Categories, ","); got != w.categories {
			t.Errorf("%s: categories %s, want %s", f, got, w.categories)
		}
	}
}

var printerColumnFilter = regexp.MustCompile(`\[\?\(@\.type=="[A-Za-z]+"\)\]`)

// A-94: every printer column's JSONPath names a field of the kind's schema
// (so a rename cannot leave an empty column), and the final column set is
// the one A-94 records.
func TestPrinterColumnsFollowTheSchema(t *testing.T) {
	want := map[string]string{
		"MysqlFailoverGroup":      "Active=.status.activeSite Ready Age",
		"MysqlDatabase":           "Group=.spec.failoverGroupRef.name Database=.spec.databaseName Owner=.status.ownerUser Phase=.status.phase Ready Policy=.spec.deletionPolicy Age",
		"MysqlBackup":             "Group=.spec.failoverGroupRef.name Profile=.spec.profileName Phase=.status.phase Ready Started=.status.startTime Completed=.status.completionTime Location=.status.artifactStorage.location Size=.status.size Age",
		"MysqlBackupVerification": "Group=.spec.failoverGroupRef.name Profile=.spec.profileName Phase=.status.phase Ready Backup=.status.backupRef.name Started=.status.startTime Completed=.status.completionTime Age",
		"MysqlStandbyCluster":     "Dump=.status.discovered.latestDump.name NewestBinlog=.status.discovered.binlogArchive.newestEventTime Ready Age",
	}
	for f, crd := range loadAPICRDFiles(t) {
		for _, v := range crd.Spec.Versions {
			var got []string
			for _, c := range v.Columns {
				switch {
				case c.Name == "Age" && c.JSONPath == ".metadata.creationTimestamp":
					got = append(got, "Age")
					continue
				case c.Name == "Ready" && c.JSONPath == `.status.conditions[?(@.type=="Ready")].status`:
					got = append(got, "Ready")
					continue
				}
				got = append(got, c.Name+"="+c.JSONPath)
				path := strings.Split(strings.TrimPrefix(printerColumnFilter.ReplaceAllString(c.JSONPath, "[]"), "."), ".")
				var segs []string
				for _, p := range path {
					if name, ok := strings.CutSuffix(p, "[]"); ok {
						segs = append(segs, name, "[]")
						continue
					}
					segs = append(segs, p)
				}
				if _, ok := v.Schema.OpenAPIV3Schema.at(segs); !ok {
					t.Errorf("%s %s: column %s reads %s, which is not in the schema", f, v.Name, c.Name, c.JSONPath)
				}
			}
			if g := strings.Join(got, " "); g != want[crd.Spec.Names.Kind] {
				t.Errorf("%s %s: columns\n  %s\nwant\n  %s", f, v.Name, g, want[crd.Spec.Names.Kind])
			}
		}
	}
}
