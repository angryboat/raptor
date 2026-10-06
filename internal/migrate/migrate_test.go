package migrate

import (
	"testing"

	"github.com/angryboat/raptor/internal/test"
	"github.com/stretchr/testify/assert"
)

func TestUp(t *testing.T) {
	conn := test.CreateDB(t)

	err := Up(t.Context(), conn)
	assert.NoError(t, err)
}
