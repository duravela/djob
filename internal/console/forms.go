package console

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
)

type CompanyField struct {
	Name     string `json:"name"`
	Label    string `json:"label"`
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
	Default  string `json:"default,omitempty"`
}
type CompanyForm struct {
	ID             string         `json:"id"`
	Label          string         `json:"label"`
	TenantID       string         `json:"tenant_id"`
	Runtime        string         `json:"runtime"`
	Handler        string         `json:"handler"`
	HandlerVersion string         `json:"handler_version"`
	BindingID      string         `json:"binding_id,omitempty"`
	WorkerRelease  string         `json:"worker_release,omitempty"`
	Resources      []string       `json:"resources,omitempty"`
	Fields         []CompanyField `json:"fields"`
	Revision       string         `json:"revision,omitempty"`
}

func LoadCompanyForms(path string) ([]CompanyForm, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open company forms")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 {
		return nil, fmt.Errorf("company forms require an operator-controlled regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil || len(data) > 65536 {
		return nil, fmt.Errorf("company forms exceed 64 KiB")
	}
	var forms []CompanyForm
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&forms) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("invalid company forms")
	}
	if len(forms) > 32 {
		return nil, fmt.Errorf("at most 32 company forms are supported")
	}
	ids := map[string]bool{}
	name := regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)
	for i := range forms {
		f := &forms[i]
		if !name.MatchString(f.ID) || ids[f.TenantID+"/"+f.ID] || f.TenantID == "" || len(f.TenantID) > 256 || f.Label == "" || len(f.Label) > 128 || f.Handler == "" || f.HandlerVersion == "" || f.Runtime == "" || len(f.Fields) > 32 || len(f.Resources) > 32 {
			return nil, fmt.Errorf("invalid company form identity or bounds")
		}
		ids[f.TenantID+"/"+f.ID] = true
		fields := map[string]bool{}
		for _, field := range f.Fields {
			if !name.MatchString(field.Name) || fields[field.Name] || field.Label == "" || len(field.Label) > 128 || len(field.Default) > 4096 {
				return nil, fmt.Errorf("invalid company field")
			}
			fields[field.Name] = true
			switch field.Type {
			case "string", "integer", "uint64", "number", "boolean", "json":
			default:
				return nil, fmt.Errorf("unsupported company field type")
			}
		}
		f.Revision = ""
		canonical, _ := json.Marshal(f)
		sum := sha256.Sum256(canonical)
		f.Revision = hex.EncodeToString(sum[:])
	}
	return forms, nil
}
func (s *Server) companyForms(w http.ResponseWriter, r *http.Request, sess *session) {
	var q struct {
		TenantID string `json:"tenant_id"`
	}
	if decode(w, r, 1024, &q) != nil {
		problem(w, 400, "provide a tenant")
		return
	}
	if sess.identity.TenantID != "" {
		if q.TenantID != "" && q.TenantID != sess.identity.TenantID {
			problem(w, 403, "credential does not permit this tenant")
			return
		}
		q.TenantID = sess.identity.TenantID
	}
	if q.TenantID == "" {
		problem(w, 400, "select a tenant")
		return
	}
	forms := []CompanyForm{}
	for _, form := range s.forms {
		if form.TenantID == q.TenantID {
			forms = append(forms, form)
		}
	}
	writeJSON(w, 200, map[string]any{"tenant_id": q.TenantID, "forms": forms})
}
