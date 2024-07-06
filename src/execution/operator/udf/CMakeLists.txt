add_library_unity(duckdb_operator_udf OBJECT physical_udf.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_udf>
    PARENT_SCOPE)
