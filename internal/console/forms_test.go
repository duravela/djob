package console

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestCompanyFormsAreBoundedOperatorConfigurationAndTenantScoped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forms.json")
	data := []byte(`[{"id":"sample","label":"Company order","tenant_id":"alpha","runtime":"steps/v1","handler":"company","handler_version":"1","fields":[{"name":"sequence","label":"Sequence","type":"uint64","required":true}]}]`)
	require.NoError(t, os.WriteFile(path, data, 0600))
	forms, err := LoadCompanyForms(path)
	require.NoError(t, err)
	require.Len(t, forms[0].Revision, 64)
	b := newBrowser(t, func(http.ResponseWriter, *http.Request) { t.Error("form inspection contacted controller") }, func(c *Config) { c.CompanyForms = forms })
	response := b.call(http.MethodPost, "forms", map[string]string{"tenant_id": "alpha"})
	require.Equal(t, 200, response.Code)
	var reply struct{ Forms []CompanyForm }
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &reply))
	require.Len(t, reply.Forms, 1)
	require.Equal(t, 403, b.call(http.MethodPost, "forms", map[string]string{"tenant_id": "beta"}).Code)
	require.NoError(t, os.Chmod(path, 0666))
	_, err = LoadCompanyForms(path)
	require.Error(t, err)
}
