package sqliteha_test

import (
	"testing"

	"github.com/litesql/go-ha/hatest"
	sqliteha "github.com/litesql/go-sqlite-ha"
)

func TestDriverSuite(t *testing.T) {
	hatest.RunDriverSuite(t, sqliteha.NewConnector, nil)
}

func TestResilienceSuite(t *testing.T) {
	hatest.RunResilienceSuite(t, "sqlite-ha")
}
