//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/udf_split.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_udf.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Optimizer;

class UDFSplit{

public:
	explicit UDFSplit() {}

	//! Perform udf split
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

private:
	//! Extract internal udf as a Logical_UDF
	void ExtractInternalUDF(LogicalOperator &op);
	//! Create a new UDFOP for op
	void AppendOperator(LogicalOperator &parent, unique_ptr<LogicalOperator> child);
	//! recurve check the child operator
	void VisitOperatorChildren(LogicalOperator &op);
	//! check the expression of the current operator
	void VisitOperatorExpression(LogicalOperator &op);
	//! check the expression, only support one to one UDF
	void VisitExpression(unique_ptr<Expression> *expression, LogicalOperator &parent, unique_ptr<LogicalOperator> *child);
	//! recurve check the child expression, only support one to one UDF
	void VisitExpressionChild(unique_ptr<Expression> *expression, LogicalOperator &parent_op, unique_ptr<LogicalOperator> *child_op);

};

} // namespace duckdb
