package render

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDefaultTheme(t *testing.T) {
	v := reflect.ValueOf(DefaultTheme)
	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).String() == "" {
			t.Errorf("DefaultTheme.%s is empty", typ.Field(i).Name)
		}
	}
}

func TestLoadTheme_PartialOverlay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "theme.json")
	if err := os.WriteFile(path, []byte(`{"ember":"#abcdef"}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := LoadTheme(path)
	if err != nil {
		t.Fatalf("LoadTheme: %v", err)
	}
	if got.Ember != "#abcdef" {
		t.Errorf("Ember = %q, want overlaid #abcdef", got.Ember)
	}
	if got.Bg != DefaultTheme.Bg {
		t.Errorf("Bg = %q, want default %q", got.Bg, DefaultTheme.Bg)
	}
}

func TestLoadTheme_BadJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte(`{not json`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := LoadTheme(path); err == nil {
		t.Fatal("expected error for bad JSON, got nil")
	}
}

func TestLoadTheme_MissingFile(t *testing.T) {
	if _, err := LoadTheme(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}
