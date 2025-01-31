#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "udf_functions_to_test.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Aggregate UDFs", "[coverage][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr, nullptr, nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	SECTION("Testing a binary aggregate UDF using only template parameters") {
		// using DOUBLEs
		REQUIRE_NOTHROW(
		    con.CreateAggregateFunction<UDFAverageFunction, udf_avg_state_t<double>, double, double>("udf_avg_double"));

		con.Query("CREATE TABLE doubles (d DOUBLE)");
		con.Query("INSERT INTO doubles VALUES (1), (2), (3), (4), (5)");
		result = con.Query("SELECT udf_avg_double(d) FROM doubles");
		REQUIRE(CHECK_COLUMN(result, 0, {3.0}));

		// using INTEGERs
		REQUIRE_NOTHROW(con.CreateAggregateFunction<UDFAverageFunction, udf_avg_state_t<int>, int, int>("udf_avg_int"));

		con.Query("CREATE TABLE integers (i INTEGER)");
		con.Query("INSERT INTO integers VALUES (1), (2), (3), (4), (5)");
		result = con.Query("SELECT udf_avg_int(i) FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
	}

	SECTION("Testing a binary aggregate UDF using only template parameters") {
		// using DOUBLEs
		con.CreateAggregateFunction<UDFCovarPopOperation, udf_covar_state_t, double, double, double>(
		    "udf_covar_pop_double");

		result = con.Query("SELECT udf_covar_pop_double(3,3), udf_covar_pop_double(NULL,3), "
		                   "udf_covar_pop_double(3,NULL), udf_covar_pop_double(NULL,NULL)");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

		// using INTEGERs
		con.CreateAggregateFunction<UDFCovarPopOperation, udf_covar_state_t, int, int, int>("udf_covar_pop_int");

		result = con.Query("SELECT udf_covar_pop_int(3,3), udf_covar_pop_int(NULL,3), udf_covar_pop_int(3,NULL), "
		                   "udf_covar_pop_int(NULL,NULL)");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	}

	SECTION("Testing aggregate UDF with arguments") {
		REQUIRE_NOTHROW(con.CreateAggregateFunction<UDFAverageFunction, udf_avg_state_t<int>, int, int>(
		    "udf_avg_int_args", LogicalType::INTEGER, LogicalType::INTEGER));

		con.Query("CREATE TABLE integers (i INTEGER)");
		con.Query("INSERT INTO integers VALUES (1), (2), (3), (4), (5)");
		result = con.Query("SELECT udf_avg_int_args(i) FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {3}));

		// using TIMEs to test disambiguation
		REQUIRE_NOTHROW(con.CreateAggregateFunction<UDFAverageFunction, udf_avg_state_t<dtime_t>, dtime_t, dtime_t>(
		    "udf_avg_time_args", LogicalType::TIME, LogicalType::TIME));
		con.Query("CREATE TABLE times (t TIME)");
		con.Query("INSERT INTO times VALUES ('01:00:00'), ('01:00:00'), ('01:00:00'), ('01:00:00'), ('01:00:00')");
		result = con.Query("SELECT udf_avg_time_args(t) FROM times");

		REQUIRE(CHECK_COLUMN(result, 0, {"01:00:00"}));

		// using DOUBLEs and a binary UDF
		con.CreateAggregateFunction<UDFCovarPopOperation, udf_covar_state_t, double, double, double>(
		    "udf_covar_pop_double_args", LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);

		result = con.Query("SELECT udf_covar_pop_double_args(3,3), udf_covar_pop_double_args(NULL,3), "
		                   "udf_covar_pop_double_args(3,NULL), udf_covar_pop_double_args(NULL,NULL)");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	}

	SECTION("Testing aggregate UDF with WRONG arguments") {
		// wrong return type
		REQUIRE_THROWS(con.CreateAggregateFunction<UDFAverageFunction, udf_avg_state_t<int>, double, int>(
		    "udf_avg_int_args", LogicalType::INTEGER, LogicalType::INTEGER));
		REQUIRE_THROWS(con.CreateAggregateFunction<UDFAverageFunction, udf_avg_state_t<int>, int, int>(
		    "udf_avg_int_args", LogicalType::DOUBLE, LogicalType::INTEGER));

		// wrong first argument
		REQUIRE_THROWS(con.CreateAggregateFunction<UDFAverageFunction, udf_avg_state_t<int>, int, double>(
		    "udf_avg_int_args", LogicalType::INTEGER, LogicalType::INTEGER));
		REQUIRE_THROWS(con.CreateAggregateFunction<UDFAverageFunction, udf_avg_state_t<int>, int, int>(
		    "udf_avg_int_args", LogicalType::INTEGER, LogicalType::DOUBLE));

		// wrong first argument
		REQUIRE_THROWS(con.CreateAggregateFunction<UDFCovarPopOperation, udf_covar_state_t, double, double, int>(
		    "udf_covar_pop_double_args", LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
		REQUIRE_THROWS(con.CreateAggregateFunction<UDFCovarPopOperation, udf_covar_state_t, double, double, double>(
		    "udf_covar_pop_double_args", LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER));
	}

	SECTION("Testing the generic CreateAggregateFunction()") {
		REQUIRE_NOTHROW(con.CreateAggregateFunction(
		    "udf_sum", {LogicalType::DOUBLE}, LogicalType::DOUBLE, &UDFSum::StateSize<UDFSum::sum_state_t>,
		    &UDFSum::Initialize<UDFSum::sum_state_t>, &UDFSum::Update<UDFSum::sum_state_t, double>,
		    &UDFSum::Combine<UDFSum::sum_state_t>, &UDFSum::Finalize<UDFSum::sum_state_t, double>,
		    &UDFSum::SimpleUpdate<UDFSum::sum_state_t, double>));

		REQUIRE_NO_FAIL(con.Query("SELECT udf_sum(1)"));
		result = con.Query("SELECT udf_sum(1)");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));

		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers SELECT * FROM range(0, 1000, 1)"));
		result = con.Query("SELECT udf_sum(i) FROM integers");
		REQUIRE(CHECK_COLUMN(result, 0, {499500}));
	}
}