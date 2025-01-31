#include "catch.hpp"
#include "test_helpers.hpp"

#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test database maximum_threads argument", "[api]") {
	// default is number of hw threads
	// FIXME: not yet
	{
		DuckDB db(nullptr, nullptr, nullptr);
		REQUIRE(db.NumberOfThreads() == std::thread::hardware_concurrency());
	}
	// but we can set another value
	{
		DBConfig config;
		config.maximum_threads = 10;
		DuckDB db(nullptr,nullptr, &config);
		REQUIRE(db.NumberOfThreads() == 10);
	}
	// zero is not erlaubt
	{
		DBConfig config;
		config.maximum_threads = 0;
		DuckDB db(nullptr, nullptr, nullptr);
		REQUIRE_THROWS(db = DuckDB(nullptr,nullptr, &config));
	}
}
