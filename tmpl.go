package eventmaster

import (
	"fmt"
	"html/template"
	"path/filepath"
	"sync"
)

// TemplateGetter defines what needs to be implemented to be able to fetch
// templates for use by a Site.
type TemplateGetter interface {
	Get(name string) (*template.Template, error)
}

// Disk keeps track of the location of the root directory of the template
// directory.
type Disk struct {
	Root string
}

// Get attempts to merge the requested name+.html with base.html found at root.
func (d Disk) Get(name string) (*template.Template, error) {
	p := filepath.Join(d.Root, name)
	return template.New("main.html").Funcs(funcMap).ParseFiles(p, filepath.Join(d.Root, "main.html"))
}

// AssetTemplate pulls templates from an assetfs.
type AssetTemplate struct {
	sync.RWMutex
	Asset func(name string) ([]byte, error)

	cache map[string]*template.Template
}

// NewAssetTemplate returns a ready-to-use AssetTemplate.
func NewAssetTemplate(asset func(string) ([]byte, error)) *AssetTemplate {
	return &AssetTemplate{
		Asset: asset,
		cache: map[string]*template.Template{},
	}
}

// Get attempts to merge the requested name+.html with base.html found at root.
func (at *AssetTemplate) Get(name string) (*template.Template, error) {
	at.RLock()
	if t, ok := at.cache[name]; ok {
		at.RUnlock()
		return t, nil
	}
	at.RUnlock()

	base, err := at.Asset("templates/main.html")
	if err != nil {
		return nil, fmt.Errorf("could not get base contents: %v", err)
	}
	page, err := at.Asset(fmt.Sprintf("templates/%s", name))
	if err != nil {
		return nil, fmt.Errorf("could not get base contents: %v", err)
	}

	t, err := template.New(name).Funcs(funcMap).Parse(string(base))
	if err != nil {
		return t, fmt.Errorf("parsing base: %v", err)
	}
	t, err = t.Parse(string(page))
	if err != nil {
		return t, fmt.Errorf("parsing specific page %s: %v", name, err)
	}

	at.Lock()
	at.cache[name] = t
	at.Unlock()

	return t, nil
}
