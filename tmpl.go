package eventmaster

import (
	"html/template"
	"path/filepath"
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
