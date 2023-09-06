package parser

import (
	"fmt"
	pgparser "github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/kanaries/gw-dsl-parser/common"
	"go/constant"
	"strconv"
	"strings"
)

// Parser is the interface for parsing DSL.
type Parser interface {
	Parse(dataset Dataset, dsl GraphicWalkerDSL) (string, error)
}

// BaseParser Source:2011, Source:2008, Source:2003, Source:1999, and Source-92 Standard
type BaseParser struct {
}

func (p BaseParser) Parse(dataset Dataset, dsl GraphicWalkerDSL) (string, error) {
	if dataset.Type == common.DatasetTypeSubQuery {
		ast, err := pgparser.ParseOne(dataset.Source)
		if err != nil {
			hlog.Errorf("parser error %s", err)
			return "", err
		}
		selectFrom := ast.AST.(*tree.Select)
		from := tree.From{
			Tables: tree.TableExprs{&tree.AliasedTableExpr{
				Expr: &tree.Subquery{
					Select: &tree.ParenSelect{
						Select: selectFrom,
					},
				},
				As: tree.AliasClause{
					Alias: tree.Name(tree.NameString("kanaries_sub_query")),
				},
			}},
		}
		return p.parseDSLToSQL(dsl, from)
	}
	table := tree.MakeUnqualifiedTableName(tree.Name(dataset.Source))
	from := tree.From{
		Tables: tree.TableExprs{&tree.AliasedTableExpr{
			Expr: &table,
		}},
	}
	return p.parseDSLToSQL(dsl, from)
}

// parser -> base parser (provide the most basic Parser logic, but support some hooks, such as GetFuncMapping)
func (p BaseParser) parseDSLToSQL(payload GraphicWalkerDSL, from tree.From) (string, error) {
	ast := &tree.Select{}
	aliasCol := make(map[string]*tree.SelectExpr)
	var whereExprList tree.Exprs
	var selectExprList tree.SelectExprs
	var subSelectExprList tree.SelectExprs
	var groupExprList tree.GroupBy
	var orderBy *tree.OrderBy
	for _, node := range payload.Workflow {
		switch node.Type {
		case common.WorkflowNodeTypeFilter:
			{
				for _, filter := range node.Filters {
					whereExprList = append(whereExprList, p.GetWhereExpr(filter))
				}
			}
		case common.WorkflowNodeTypeTransform:
			{
				for _, transform := range node.Transform {
					var expr *tree.SelectExpr
					var subExprs tree.SelectExprs
					expr, subExprs, aliasCol = p.GetSelectExpr(transform, aliasCol)
					if subExprs != nil {
						for _, subExpr := range subExprs {
							subSelectExprList = append(subSelectExprList, subExpr)
						}
					}
					if expr != nil {
						selectExprList = append(selectExprList, *expr)
					}
				}
			}
		case common.WorkflowNodeTypeView:
			{
				for _, query := range node.Query {
					var viewSelect tree.SelectExprs
					var viewGroup tree.GroupBy
					viewSelect, viewGroup, aliasCol = p.GetSelectAndGroupExprList(query, aliasCol)
					selectExprList = viewSelect
					groupExprList = viewGroup
				}
			}
		case common.WorkflowNodeTypeSort:
			{
				var orderByExpr tree.OrderBy
				if node.Descending() != nil {
					var direction tree.Direction
					if *node.Descending() {
						direction = tree.Descending
					} else {
						direction = tree.Ascending
					}
					for _, sort := range node.By {
						sortExpr := tree.Order{
							OrderType: tree.OrderByColumn,
							Direction: direction,
							Expr:      tree.NewUnresolvedName(sort),
						}
						orderByExpr = append(orderByExpr, &sortExpr)
					}
					orderBy = &orderByExpr
				}
			}
		}
	}

	if len(selectExprList) == 0 {
		selectExprList = append(selectExprList, tree.SelectExpr{})
	}

	//limit
	limitNode := tree.Limit{}
	if payload.Limit != 0 {
		offset := payload.Offset
		limitNode = tree.Limit{
			Count:  tree.NewDInt(tree.DInt(payload.Limit)),
			Offset: tree.NewDInt(tree.DInt(offset)),
		}
	}

	//order by
	if orderBy != nil {
		ast.OrderBy = *orderBy
	}

	//where
	var where *tree.Where
	if len(whereExprList) != 0 {
		where = tree.NewWhere(tree.AstWhere, getWhereFromExprList(whereExprList))
	}

	// process sub select
	if len(subSelectExprList) != 0 {
		switch expr := from.Tables[0].(*tree.AliasedTableExpr).Expr.(type) {
		case *tree.Subquery:
			{
				subSelect := append(expr.Select.(*tree.ParenSelect).Select.Select.(*tree.SelectClause).Exprs, subSelectExprList...)
				from.Tables[0].(*tree.AliasedTableExpr).Expr.(*tree.Subquery).Select.(*tree.ParenSelect).Select.Select.(*tree.SelectClause).Exprs = subSelect
			}
		case *tree.TableName:
			{
				subSelect := append(tree.SelectExprs{tree.StarSelectExpr()}, subSelectExprList...)
				from = tree.From{
					Tables: tree.TableExprs{&tree.AliasedTableExpr{
						Expr: &tree.Subquery{
							Select: &tree.ParenSelect{
								Select: &tree.Select{
									Select: &tree.SelectClause{
										Exprs: subSelect,
										From: tree.From{
											Tables: tree.TableExprs{&tree.AliasedTableExpr{Expr: expr}},
										},
									},
								},
							},
						},
						As: tree.AliasClause{
							Alias: tree.Name(tree.NameString("kanaries_sub_query")),
						},
					}},
				}
			}
		}
	}

	ast.Select = &tree.SelectClause{
		Exprs:   selectExprList,
		Where:   where,
		From:    from,
		GroupBy: groupExprList,
	}
	ast.Limit = &limitNode

	finalStr := ast.String()
	return finalStr, nil
}

func (BaseParser) GetWhereExpr(filter Filter) tree.Expr {
	switch filter.Rule.Type {
	case RANGE, TEMPORAL_RANGE:
		{
			expr := &tree.RangeCond{
				Not:       false,
				Symmetric: false,
			}
			if len(filter.Key) != 0 {
				expr.Left = tree.NewUnresolvedName(filter.Key)
			}
			if len(filter.Fid) != 0 {
				expr.Left = tree.NewUnresolvedName(filter.Fid)
			}
			// process any type
			fromValue := filter.Rule.Value[0]
			toValue := filter.Rule.Value[1]
			var fromExpr, toExpr tree.Expr
			if _, ok := fromValue.(int); ok {
				fromExpr = tree.NewNumVal(constant.MakeInt64(fromValue.(int64)), fromValue.(string), false)
				toExpr = tree.NewNumVal(constant.MakeInt64(toValue.(int64)), toValue.(string), false)
			} else if _, ok := fromValue.(float64); ok {
				fromExpr = tree.NewNumVal(constant.MakeFloat64(fromValue.(float64)), strconv.FormatFloat(fromValue.(float64), 'f', 4, 64), false)
				toExpr = tree.NewNumVal(constant.MakeFloat64(toValue.(float64)), strconv.FormatFloat(toValue.(float64), 'f', 4, 64), false)
			} else {
				fromExpr = tree.NewStrVal(fromValue.(string))
				toExpr = tree.NewStrVal(toValue.(string))
			}
			expr.From = fromExpr
			expr.To = toExpr
			return expr
		}
	case ONE_OF:
		{
			var exprs = make([]tree.Expr, 0)
			value := filter.Rule.Value[0]
			if _, ok := value.(int); ok {
				for _, v := range filter.Rule.Value {
					expr := tree.NewNumVal(constant.MakeInt64(v.(int64)), v.(string), false)
					exprs = append(exprs, expr)
				}
			} else if _, ok := value.(float64); ok {
				for _, v := range filter.Rule.Value {
					expr := tree.NewNumVal(constant.MakeFloat64(v.(float64)), strconv.FormatFloat(v.(float64), 'f', 4, 64), false)
					exprs = append(exprs, expr)
				}
			} else {
				for _, v := range filter.Rule.Value {
					expr := tree.NewStrVal(v.(string))
					exprs = append(exprs, expr)
				}
			}
			expr := &tree.ComparisonExpr{
				Operator:    tree.In,
				SubOperator: tree.EQ,
			}
			if len(filter.Key) != 0 {
				expr.Left = tree.NewUnresolvedName(filter.Key)
			}
			if len(filter.Fid) != 0 {
				expr.Left = tree.NewUnresolvedName(filter.Fid)
			}
			//todo https://github.com/auxten/postgresql-parser/issues/26
			if len(exprs) == 1 {
				exprs = append(exprs, exprs[0])
			}
			expr.Right = &tree.Tuple{
				Exprs: exprs,
			}
			return expr
		}
	}
	return nil
}

func (base BaseParser) GetSelectExpr(transform Transform, existCol map[string]*tree.SelectExpr) (*tree.SelectExpr, tree.SelectExprs, map[string]*tree.SelectExpr) {
	var expr *tree.SelectExpr
	var subExprs tree.SelectExprs
	switch transform.Expression.Op {
	case "bin":
		param := transform.Expression.Params[0]
		num := transform.Expression.Num
		if num <= 0 {
			num = 10
		}
		minNum := num - 1
		expr = &tree.SelectExpr{
			Expr: &tree.BinaryExpr{
				Operator: tree.Plus,
				Left:     tree.NewUnresolvedName(fmt.Sprintf("min_%s", transform.Expression.As)),
				Right: &tree.BinaryExpr{
					Operator: tree.Mult,
					Left: &tree.FuncExpr{
						Func: tree.ResolvableFunctionReference{
							FunctionReference: &tree.FunctionDefinition{
								Name: "least",
							},
						},
						Exprs: tree.Exprs{
							&tree.FuncExpr{
								Func: tree.ResolvableFunctionReference{
									FunctionReference: tree.NewUnresolvedName("floor"),
								},
								Exprs: tree.Exprs{
									&tree.BinaryExpr{
										Operator: tree.Div,
										Left: &tree.ParenExpr{
											Expr: &tree.BinaryExpr{
												Operator: tree.Minus,
												Left:     tree.NewUnresolvedName(param.Value),
												Right:    tree.NewUnresolvedName(fmt.Sprintf("min_%s", transform.Expression.As)),
											},
										},
										Right: &tree.ParenExpr{
											Expr: &tree.BinaryExpr{
												Operator: tree.Div,
												Left: &tree.ParenExpr{
													Expr: &tree.BinaryExpr{
														Operator: tree.Minus,
														Left:     tree.NewUnresolvedName(fmt.Sprintf("max_%s", transform.Expression.As)),
														Right:    tree.NewUnresolvedName(fmt.Sprintf("min_%s", transform.Expression.As)),
													},
												},
												Right: tree.NewNumVal(constant.MakeFloat64(float64(num)), strconv.FormatFloat(float64(num), 'f', 1, 64), false),
											},
										},
									},
								},
							},
							tree.NewNumVal(constant.MakeInt64(minNum), strconv.FormatInt(minNum, 10), false),
						},
					},
					Right: &tree.ParenExpr{
						Expr: &tree.BinaryExpr{
							Operator: tree.Div,
							Left: &tree.ParenExpr{
								Expr: &tree.BinaryExpr{
									Operator: tree.Minus,
									Left:     tree.NewUnresolvedName(fmt.Sprintf("max_%s", transform.Expression.As)),
									Right:    tree.NewUnresolvedName(fmt.Sprintf("min_%s", transform.Expression.As)),
								},
							},
							Right: tree.NewNumVal(constant.MakeFloat64(float64(num)), strconv.FormatFloat(float64(num), 'f', 1, 64), false),
						},
					},
				},
			},
			As: tree.UnrestrictedName(transform.Expression.As),
		}
		subExprs = []tree.SelectExpr{
			{
				Expr: &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.NewUnresolvedName("max"),
					},
					WindowDef: &tree.WindowDef{},
					Exprs: tree.Exprs{
						tree.NewUnresolvedName(param.Value),
					},
				},
				As: tree.UnrestrictedName(fmt.Sprintf("max_%s", transform.Expression.As)),
			},
			{
				Expr: &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.NewUnresolvedName("min"),
					},
					WindowDef: &tree.WindowDef{},
					Exprs: tree.Exprs{
						tree.NewUnresolvedName(param.Value),
					},
				},
				As: tree.UnrestrictedName(fmt.Sprintf("min_%s", transform.Expression.As)),
			},
		}
	case "binCount":
		num := transform.Expression.Num
		if num <= 0 {
			num = 10
		}
		minNum := num - 1
		param := transform.Expression.Params[0]
		expr = &tree.SelectExpr{
			Expr: &tree.BinaryExpr{
				Operator: tree.Plus,
				Left: &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: &tree.FunctionDefinition{
							Name: "least",
						},
					},
					Exprs: tree.Exprs{
						&tree.BinaryExpr{
							Operator: tree.Div,
							Left: &tree.ParenExpr{
								Expr: &tree.BinaryExpr{
									Operator: tree.Minus,
									Left:     tree.NewUnresolvedName(param.Value),
									Right:    tree.NewUnresolvedName(fmt.Sprintf("min_%s", transform.Expression.As)),
								},
							},
							Right: &tree.ParenExpr{
								Expr: &tree.BinaryExpr{
									Operator: tree.Div,
									Left: &tree.ParenExpr{
										Expr: &tree.BinaryExpr{
											Operator: tree.Minus,
											Left:     tree.NewUnresolvedName(fmt.Sprintf("max_%s", transform.Expression.As)),
											Right:    tree.NewUnresolvedName(fmt.Sprintf("min_%s", transform.Expression.As)),
										},
									},
									Right: tree.NewNumVal(constant.MakeInt64(num), strconv.FormatInt(num, 10), false),
								},
							},
						},
						tree.NewNumVal(constant.MakeInt64(minNum), strconv.FormatInt(minNum, 10), false),
					},
				},
				Right: tree.NewNumVal(constant.MakeInt64(1), "1", false),
			},
			As: tree.UnrestrictedName(transform.Expression.As),
		}
		subExprs = []tree.SelectExpr{
			{
				Expr: &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.NewUnresolvedName("max"),
					},
					WindowDef: &tree.WindowDef{},
					Exprs: tree.Exprs{
						tree.NewUnresolvedName(param.Value),
					},
				},
				As: tree.UnrestrictedName(fmt.Sprintf("max_%s", transform.Expression.As)),
			},
			{
				Expr: &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.NewUnresolvedName("min"),
					},
					WindowDef: &tree.WindowDef{},
					Exprs: tree.Exprs{
						tree.NewUnresolvedName(param.Value),
					},
				},
				As: tree.UnrestrictedName(fmt.Sprintf("min_%s", transform.Expression.As)),
			},
		}
	case "log2":
		param := transform.Expression.Params[0]
		expr = &tree.SelectExpr{
			Expr: &tree.BinaryExpr{
				Operator: tree.Div,
				Left: &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.NewUnresolvedName("log"),
					},
					Exprs: tree.Exprs{
						tree.NewUnresolvedName(param.Value),
					},
				},
				Right: &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.NewUnresolvedName("log"),
					},
					Exprs: tree.Exprs{
						tree.NewNumVal(constant.MakeInt64(2), "2", false),
					},
				},
			},
			As: tree.UnrestrictedName(transform.Expression.As),
		}
	case "log10":
		param := transform.Expression.Params[0]
		expr = &tree.SelectExpr{
			Expr: &tree.FuncExpr{
				Func: tree.ResolvableFunctionReference{
					FunctionReference: tree.NewUnresolvedName("log10"),
				},
				Exprs: tree.Exprs{
					tree.NewUnresolvedName(param.Value),
				},
			},
			As: tree.UnrestrictedName(transform.Expression.As),
		}
	case "one":
		expr = &tree.SelectExpr{
			Expr: tree.NewNumVal(constant.MakeInt64(1), "1", false),
			As:   tree.UnrestrictedName(transform.Expression.As),
		}
	case "sql":
		param := transform.Expression.Params[0]
		expr = &tree.SelectExpr{
			Expr: &SqlExprVal{
				Str: param.Value,
			},
			As: tree.UnrestrictedName(transform.Expression.As),
		}
	case "log":
		param := transform.Expression.Params[0]
		num := transform.Expression.Num
		expr = &tree.SelectExpr{
			Expr: &tree.BinaryExpr{
				Operator: tree.Div,
				Left: &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.NewUnresolvedName("log"),
					},
					Exprs: tree.Exprs{
						tree.NewUnresolvedName(param.Value),
					},
				},
				Right: &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.NewUnresolvedName("log"),
					},
					Exprs: tree.Exprs{
						tree.NewNumVal(constant.MakeInt64(num), strconv.FormatInt(num, 10), false),
					},
				},
			},
			As: tree.UnrestrictedName(transform.Expression.As),
		}
	case "dateTimeDrill":
		ast, _ := pgparser.ParseOne("SELECT TO_CHAR(\n  DATE_TRUNC('year', to_timestamp(col_1, 'YYYY-MM-DD')),\n  'YYYY'\n)")
		println(ast.AST.String())
		field := ""
		value := ""
		for _, param := range transform.Expression.Params {
			if param.Type == "field" {
				field = param.Value
			}
			if param.Type == "value" {
				value = param.Value
			}
		}
		formatExpr, truncExpr := base.GetDataTruncExpr(value)
		expr = &tree.SelectExpr{
			Expr: &tree.FuncExpr{
				Func: tree.ResolvableFunctionReference{
					FunctionReference: tree.NewUnresolvedName("to_char"),
				},
				Exprs: tree.Exprs{
					&tree.FuncExpr{
						Func: tree.ResolvableFunctionReference{
							FunctionReference: tree.NewUnresolvedName("date_trunc"),
						},
						Exprs: tree.Exprs{
							tree.NewUnresolvedName(field),
							tree.NewStrVal(truncExpr),
						},
					},
					tree.NewStrVal(formatExpr),
				},
			},
			As: tree.UnrestrictedName(transform.Expression.As),
		}
	}
	existCol[transform.Expression.As] = expr

	return expr, subExprs, existCol
}

func (p BaseParser) GetSelectAndGroupExprList(query Query, aliasCol map[string]*tree.SelectExpr) (tree.SelectExprs, tree.GroupBy, map[string]*tree.SelectExpr) {
	var selectExprs tree.SelectExprs
	var groupExprs tree.GroupBy
	switch query.Op {
	case "aggregate":
		{
			for _, key := range query.GroupBy {
				groupExprs = append(groupExprs, tree.NewUnresolvedName(key))
				if aliasCol[key] == nil {
					selectExprs = append(selectExprs, tree.SelectExpr{
						Expr: &SqlExprVal{
							Str: key,
						},
					})
				} else {
					selectExprs = append(selectExprs, *aliasCol[key])
				}
			}
			for _, measure := range query.Measures {
				aggFunc := p.GetAggFunc(measure.Agg)
				// process alias
				filed := measure.Field
				var nameExpr tree.Expr
				if filed == "*" {
					nameExpr = tree.UnqualifiedStar{}
				} else {
					nameExpr = tree.NewUnresolvedName(filed)
				}
				if aliasCol[filed] != nil {
					nameExpr = aliasCol[filed].Expr
				}
				if aggFunc != "" {
					if aggFunc == "median" {
						namePart := fmt.Sprintf("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY %s)", filed)
						selectExprs = append(selectExprs, tree.SelectExpr{
							Expr: &SqlExprVal{
								Str: namePart,
							},
							As: tree.UnrestrictedName(measure.AsFieldKey),
						})
					} else {
						selectExpr := tree.SelectExpr{
							Expr: &tree.FuncExpr{
								Func: tree.ResolvableFunctionReference{
									FunctionReference: tree.NewUnresolvedName(aggFunc),
								},
								Exprs: tree.Exprs{
									nameExpr,
								},
							},
							As: tree.UnrestrictedName(measure.AsFieldKey),
						}
						selectExprs = append(selectExprs, selectExpr)
					}
				}
			}
			return selectExprs, groupExprs, aliasCol
		}
	case "raw":
		{
			if len(query.Fields) != 0 {
				for _, fid := range query.Fields {
					if aliasCol[fid] == nil {
						if fid == "*" {
							selectExprs = append(selectExprs, tree.SelectExpr{
								Expr: tree.UnqualifiedStar{},
							})
						} else {
							fid := strings.Trim(fid, "\"")
							selectExprs = append(selectExprs, tree.SelectExpr{
								Expr: tree.NewUnresolvedName(fid),
							})
						}
					} else {
						selectExprs = append(selectExprs, *aliasCol[fid])
					}
				}
				return selectExprs, nil, aliasCol
			}
		}
	default:
		{
			return nil, nil, aliasCol
		}
	}
	return nil, nil, aliasCol
}

func (BaseParser) GetAggFunc(agg IAggregator) string {
	switch agg {
	case Sum:
		return "sum"
	case Count:
		return "count"
	case Max:
		return "max"
	case Min:
		return "min"
	case Mean:
		return "avg"
	case Median:
		return "median"
	case Variance:
		return "variance"
	case Stdev:
		return "stddev"
	default:
		return ""
	}
}

func (BaseParser) GetDataTruncExpr(funcType string) (formatExpr, dataTruncExpr string) {
	switch funcType {
	case "year":
		formatExpr = "YYYY"
		dataTruncExpr = "year"
	case "month":
		formatExpr = "YYYY-MM"
		dataTruncExpr = "month"
	case "week":
		formatExpr = "YYYY-MM-DD"
		dataTruncExpr = "week"
	case "day":
		formatExpr = "YYYY-MM-DD"
		dataTruncExpr = "day"
	case "hour":
		formatExpr = "YYYY-MM-DD HH24"
		dataTruncExpr = "hour"
	case "minute":
		formatExpr = "YYYY-MM-DD HH24:MI"
		dataTruncExpr = "minute"
	case "second":
		formatExpr = "YYYY-MM-DD HH24:MI:SS"
		dataTruncExpr = "second"
	default:
		formatExpr = "YYYY-MM-DD HH24:MI:SS"
		dataTruncExpr = "second"
	}
	return
}
func getWhereFromExprList(exprList tree.Exprs) tree.Expr {
	if len(exprList) == 1 {
		return exprList[0]
	}
	return &tree.AndExpr{
		Left:  exprList[0],
		Right: getWhereFromExprList(exprList[1:]),
	}
}
