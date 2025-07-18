//===----------------------------------------------------------------------===//
//                         DuckDB
//
// core_functions/scalar/generic_functions.hpp
//
//
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_functions.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct AliasFun {
	static constexpr const char *Name = "alias";
	static constexpr const char *Parameters = "expr";
	static constexpr const char *Description = "Returns the name of a given expression";
	static constexpr const char *Example = "alias(42 + 1)";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct CurrentSettingFun {
	static constexpr const char *Name = "current_setting";
	static constexpr const char *Parameters = "setting_name";
	static constexpr const char *Description = "Returns the current value of the configuration setting";
	static constexpr const char *Example = "current_setting('access_mode')";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct HashFun {
	static constexpr const char *Name = "hash";
	static constexpr const char *Parameters = "value";
	static constexpr const char *Description = "Returns a `UBIGINT` with the hash of the `value`. Note that this is not a cryptographic hash.";
	static constexpr const char *Example = "hash('🦆')";
	static constexpr const char *Categories = "string";

	static ScalarFunction GetFunction();
};

struct LeastFun {
	static constexpr const char *Name = "least";
	static constexpr const char *Parameters = "arg1,arg2,...";
	static constexpr const char *Description = "Returns the smallest value. For strings lexicographical ordering is used. Note that uppercase characters are considered “smaller” than lowercase characters, and collations are not supported.";
	static constexpr const char *Example = "least(42, 84)\002least('abc', 'bcd', 'cde', 'EFG')";
	static constexpr const char *Categories = "string,numeric,date,timestamp,aggregate";

	static ScalarFunctionSet GetFunctions();
};

struct GreatestFun {
	static constexpr const char *Name = "greatest";
	static constexpr const char *Parameters = "arg1,arg2,...";
	static constexpr const char *Description = "Returns the largest value. For strings lexicographical ordering is used. Note that lowercase characters are considered “larger” than uppercase characters and collations are not supported.";
	static constexpr const char *Example = "greatest(42, 84)\002greatest('abc', 'bcd', 'cde', 'EFG')";
	static constexpr const char *Categories = "string,numeric,date,timestamp,aggregate";

	static ScalarFunctionSet GetFunctions();
};

struct StatsFun {
	static constexpr const char *Name = "stats";
	static constexpr const char *Parameters = "expression";
	static constexpr const char *Description = "Returns a string with statistics about the expression. Expression can be a column, constant, or SQL expression";
	static constexpr const char *Example = "stats(5)";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct TypeOfFun {
	static constexpr const char *Name = "typeof";
	static constexpr const char *Parameters = "expression";
	static constexpr const char *Description = "Returns the name of the data type of the result of the expression";
	static constexpr const char *Example = "typeof('abc')";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct CanCastImplicitlyFun {
	static constexpr const char *Name = "can_cast_implicitly";
	static constexpr const char *Parameters = "source_type,target_type";
	static constexpr const char *Description = "Whether or not we can implicitly cast from the source type to the other type";
	static constexpr const char *Example = "can_cast_implicitly(NULL::INTEGER, NULL::BIGINT)";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct CurrentQueryFun {
	static constexpr const char *Name = "current_query";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "Returns the current query as a string";
	static constexpr const char *Example = "current_query()";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct CurrentSchemaFun {
	static constexpr const char *Name = "current_schema";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "Returns the name of the currently active schema. Default is main";
	static constexpr const char *Example = "current_schema()";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct CurrentSchemasFun {
	static constexpr const char *Name = "current_schemas";
	static constexpr const char *Parameters = "include_implicit";
	static constexpr const char *Description = "Returns list of schemas. Pass a parameter of True to include implicit schemas";
	static constexpr const char *Example = "current_schemas(true)";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct CurrentDatabaseFun {
	static constexpr const char *Name = "current_database";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "Returns the name of the currently active database";
	static constexpr const char *Example = "current_database()";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct InSearchPathFun {
	static constexpr const char *Name = "in_search_path";
	static constexpr const char *Parameters = "database_name,schema_name";
	static constexpr const char *Description = "Returns whether or not the database/schema are in the search path";
	static constexpr const char *Example = "in_search_path('memory', 'main')";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct CurrentTransactionIdFun {
	static constexpr const char *Name = "txid_current";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "Returns the current transaction’s ID (a BIGINT). It will assign a new one if the current transaction does not have one already";
	static constexpr const char *Example = "txid_current()";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct VersionFun {
	static constexpr const char *Name = "version";
	static constexpr const char *Parameters = "";
	static constexpr const char *Description = "Returns the currently active version of DuckDB in this format: v0.3.2	";
	static constexpr const char *Example = "version()";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct EquiWidthBinsFun {
	static constexpr const char *Name = "equi_width_bins";
	static constexpr const char *Parameters = "min,max,bin_count,nice_rounding";
	static constexpr const char *Description = "Generates bin_count equi-width bins between the min and max. If enabled nice_rounding makes the numbers more readable/less jagged";
	static constexpr const char *Example = "equi_width_bins(0, 10, 2, true)";
	static constexpr const char *Categories = "";

	static ScalarFunctionSet GetFunctions();
};

struct IsHistogramOtherBinFun {
	static constexpr const char *Name = "is_histogram_other_bin";
	static constexpr const char *Parameters = "val";
	static constexpr const char *Description = "Whether or not the provided value is the histogram \"other\" bin (used for values not belonging to any provided bin)";
	static constexpr const char *Example = "is_histogram_other_bin(v)";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct CastToTypeFun {
	static constexpr const char *Name = "cast_to_type";
	static constexpr const char *Parameters = "param,type";
	static constexpr const char *Description = "Casts the first argument to the type of the second argument";
	static constexpr const char *Example = "cast_to_type('42', NULL::INTEGER)";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

struct ReplaceTypeFun {
	static constexpr const char *Name = "replace_type";
	static constexpr const char *Parameters = "param,type1,type2";
	static constexpr const char *Description = "Casts all fields of type1 to type2";
	static constexpr const char *Example = "replace_type({duck: 3.141592653589793::DOUBLE}, NULL::DOUBLE, NULL::DECIMAL(15,2))";
	static constexpr const char *Categories = "";

	static ScalarFunction GetFunction();
};

} // namespace duckdb
