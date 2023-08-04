package parser

import (
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/auxten/postgresql-parser/pkg/sql/types"
)

// SqlExprVal is an implementation of Expr that holds a string.
type SqlExprVal struct {
	Str string
}

func (expr *SqlExprVal) String() string {
	panic("implement me")
}

func (expr *SqlExprVal) Walk(visitor tree.Visitor) tree.Expr {
	//TODO implement me
	panic("implement me")
}

func (expr *SqlExprVal) TypeCheck(ctx *tree.SemaContext, desired *types.T) (tree.TypedExpr, error) {
	panic("implement me")
}

// RawString retrieves the underlying string of the StrVal.
func (expr *SqlExprVal) RawString() string {
	return expr.Str
}

// Format implements the NodeFormatter interface.
func (expr *SqlExprVal) Format(ctx *tree.FmtCtx) {
	buf := &ctx.Buffer
	//sql to byte[]
	bytes := []byte(expr.Str)
	buf.Write(bytes)
}
