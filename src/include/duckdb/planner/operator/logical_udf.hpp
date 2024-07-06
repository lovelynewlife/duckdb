//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_udf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"


namespace duckdb {

//! LogicalUDF represents a udf operation (only use in rewrite)
class LogicalUDF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_UDF;

public:
	explicit LogicalUDF(unique_ptr<Expression> expression);
	LogicalUDF();

public:
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
