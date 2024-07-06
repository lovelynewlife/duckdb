#include "duckdb/optimizer/udf_split.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> UDFSplit::Rewrite(unique_ptr<LogicalOperator> op) {
	// find the udf in op exepression
	// replace the UDF as op
	VisitOperatorChildren(*op);
	return op;
}

void UDFSplit::VisitOperatorChildren(LogicalOperator &op) {
	for (auto &child_op : op.children) {
		VisitOperatorChildren(*child_op);
	}
	VisitOperatorExpression(op);
}

void UDFSplit::VisitOperatorExpression(LogicalOperator &op) {
	if (op.children.size() == 0) {
		return;
	}
	auto &tmp_child = op.children[0];
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalExpressionGet>();
		for (auto &expr_list : order.expressions) {
			for (auto &expr : expr_list) {
				VisitExpression(&expr, op, &tmp_child);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &order1 = op.Cast<LogicalTopN>();
		for (auto &node : order1.orders) {
			VisitExpression(&node.expression, op, &tmp_child);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		for (auto &target : distinct.distinct_targets) {
			VisitExpression(&target, op, &tmp_child);
		}
		if (distinct.order_by) {
			for (auto &order2 : distinct.order_by->orders) {
				VisitExpression(&order2.expression, op, &tmp_child);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		for (auto &group : aggr.groups) {
			VisitExpression(&group, op, &tmp_child);
		}
		break;
	}
	default:
		break;
	}
	for (auto &expression : op.expressions) {
		VisitExpression(&expression, op, &tmp_child);
	}
}

void UDFSplit::VisitExpression(unique_ptr<Expression> *expression, LogicalOperator &parent,
                               unique_ptr<LogicalOperator> *child) {
	bool recurve = false;
	auto &expr = **expression;
	switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_AGGREGATE:
		case ExpressionClass::BOUND_BETWEEN:
		case ExpressionClass::BOUND_CASE:
		case ExpressionClass::BOUND_CAST:
		case ExpressionClass::BOUND_COMPARISON:
		case ExpressionClass::BOUND_CONJUNCTION:
		case ExpressionClass::BOUND_COLUMN_REF:
		case ExpressionClass::BOUND_CONSTANT:
		case ExpressionClass::BOUND_SUBQUERY:
		case ExpressionClass::BOUND_OPERATOR:
		case ExpressionClass::BOUND_PARAMETER:
		case ExpressionClass::BOUND_REF:
		case ExpressionClass::BOUND_DEFAULT:
		case ExpressionClass::BOUND_WINDOW:
		case ExpressionClass::BOUND_UNNEST:
			recurve = true;
			break;
		case ExpressionClass::BOUND_FUNCTION: {
			bool has_udf =
				expr.Cast<BoundFunctionExpression>().function.null_handling == FunctionNullHandling::UDF_HANDLING;
			if (has_udf) {
				// insert Logical UDF operator
				AppendOperator(parent, std::move(*child));
				// let UDF operator as parent
				auto &new_parent = parent.children.back();
				auto &new_child = new_parent->children[0];
				VisitExpressionChild(expression, *new_parent, &new_child);
			}else{
				recurve = true;
			}
			break;
		}
		default:
			throw InternalException("Unrecognized expression type in UDFSplit");
	}
	if (recurve) {
		// visit the children of this node
		VisitExpressionChild(expression, parent, child);
	}
}

void UDFSplit::VisitExpressionChild(unique_ptr<Expression> *expression, LogicalOperator &parent_op,
                                    unique_ptr<LogicalOperator> *child_op) {
	auto &expr = **expression;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggr_expr = expr.Cast<BoundAggregateExpression>();
		for (auto &child : aggr_expr.children) {
			VisitExpression(&child, parent_op, child_op);
		}
		if (aggr_expr.filter) {
			VisitExpression(&aggr_expr.filter, parent_op, child_op);
		}
		if (aggr_expr.order_bys) {
			for (auto &order : aggr_expr.order_bys->orders) {
				VisitExpression(&order.expression, parent_op, child_op);
			}
		}
		break;
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between_expr = expr.Cast<BoundBetweenExpression>();
		VisitExpression(&between_expr.input, parent_op, child_op);
		VisitExpression(&between_expr.lower, parent_op, child_op);
		VisitExpression(&between_expr.upper, parent_op, child_op);
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		for (auto &case_check : case_expr.case_checks) {
			VisitExpression(&case_check.when_expr, parent_op, child_op);
			VisitExpression(&case_check.then_expr, parent_op, child_op);
		}
		VisitExpression(&case_expr.else_expr, parent_op, child_op);
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast_expr = expr.Cast<BoundCastExpression>();
		VisitExpression(&cast_expr.child, parent_op, child_op);
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = expr.Cast<BoundComparisonExpression>();
		VisitExpression(&comp_expr.left, parent_op, child_op);
		VisitExpression(&comp_expr.right, parent_op, child_op);
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conj_expr.children) {
			VisitExpression(&child, parent_op, child_op);
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		for (auto &child : func_expr.children) {
			VisitExpression(&child, parent_op, child_op);
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		for (auto &child : op_expr.children) {
			VisitExpression(&child, parent_op, child_op);
		}
		break;
	}
	case ExpressionClass::BOUND_SUBQUERY: {
		auto &subquery_expr = expr.Cast<BoundSubqueryExpression>();
		if (subquery_expr.child) {
			VisitExpression(&subquery_expr.child, parent_op, child_op);
		}
		break;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window_expr = expr.Cast<BoundWindowExpression>();
		for (auto &partition : window_expr.partitions) {
			VisitExpression(&partition, parent_op, child_op);
		}
		for (auto &order : window_expr.orders) {
			VisitExpression(&order.expression, parent_op, child_op);
		}
		for (auto &child : window_expr.children) {
			VisitExpression(&child, parent_op, child_op);
		}
		if (window_expr.filter_expr) {
			VisitExpression(&window_expr.filter_expr, parent_op, child_op);
		}
		if (window_expr.start_expr) {
			VisitExpression(&window_expr.start_expr, parent_op, child_op);
		}
		if (window_expr.end_expr) {
			VisitExpression(&window_expr.end_expr, parent_op, child_op);
		}
		if (window_expr.offset_expr) {
			VisitExpression(&window_expr.offset_expr, parent_op, child_op);
		}
		if (window_expr.default_expr) {
			VisitExpression(&window_expr.default_expr, parent_op, child_op);
		}
		break;
	}
	case ExpressionClass::BOUND_UNNEST: {
		auto &unnest_expr = expr.Cast<BoundUnnestExpression>();
		VisitExpression(&unnest_expr.child, parent_op, child_op);
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		// these node types have no children
		break;
	default:
		throw InternalException("ExpressionIterator used on unbound expression");
	}
}

void UDFSplit::AppendOperator(LogicalOperator &parent, unique_ptr<LogicalOperator> child) {
	auto udf_operator = make_uniq<LogicalUDF>();
	// insert the child into UDF op
	udf_operator->children.push_back(std::move(child));
	// insert the UDF op into parent
	if(parent.children[0] == nullptr){
		parent.children.pop_back();
	}
	parent.children.push_back(std::move(udf_operator));
}

} // namespace duckdb
