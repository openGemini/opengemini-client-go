package opengemini

type FunctionEnum string

const (
	FunctionMean  FunctionEnum = "MEAN"
	FunctionCount FunctionEnum = "COUNT"
	FunctionSum   FunctionEnum = "SUM"
	FunctionMin   FunctionEnum = "MIN"
	FunctionMax   FunctionEnum = "MAX"
	FunctionTime  FunctionEnum = "TIME"
	FunctionTop   FunctionEnum = "TOP"
	FunctionLast  FunctionEnum = "LAST"
)
