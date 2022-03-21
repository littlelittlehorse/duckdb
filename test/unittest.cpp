//#define CATCH_CONFIG_RUNNER

#include "duckdb/common/file_system.hpp"
//#include "test_helpers.hpp"

#include "duckdb.hpp"

using namespace duckdb;


void startup() {
	DBConfig config;
	string db_path = "duckdb_benchmark_db.db";
	string nvm_db_path = "nvm_duckdb_benchmark_db.db";
	config.maximum_memory = 134217728;
//	DuckDB db(db_path, &config);

	// Add nvm_db_path
//	DuckDB db(db_path, &config);
	DuckDB db(db_path, nvm_db_path, &config);

	Connection con(db);

//	con.Query("CREATE TABLE integers(i INTEGER)");
//	con.Query("INSERT INTO integers VALUES (3)");
//	con.Query("UPDATE integers SET INTEGER = 2");
	auto result = con.Query("SELECT * FROM integers");
	result->Print();
}

int main(int argc, char *argv[]) {
	startup();
}

//int main(int argc, char *argv[]) {
//
//	startup();
//
//	string test_directory = DUCKDB_ROOT_DIRECTORY;
//
//	int new_argc = 0;
//	auto new_argv = unique_ptr<char *[]>(new char *[argc]);
//	for (int i = 0; i < argc; i++) {
//		if (string(argv[i]) == "--force-storage") {
//			test_force_storage = true;
//		} else if (string(argv[i]) == "--test-dir") {
//			test_directory = string(argv[++i]);
//		} else {
//			new_argv[new_argc] = argv[i];
//			new_argc++;
//		}
//	}
//
//	TestChangeDirectory(test_directory);
//	// delete the testing directory if it exists
//	auto dir = TestCreatePath("");
//	try {
//		TestDeleteDirectory(dir);
//		// create the empty testing directory
//		TestCreateDirectory(dir);
//	} catch (std::exception &ex) {
//		fprintf(stderr, "Failed to create testing directory \"%s\": %s", dir.c_str(), ex.what());
//		return 1;
//	}
//
//	RegisterSqllogictests();
//
//	int result = Catch::Session().run(new_argc, new_argv.get());
//
//	TestDeleteDirectory(dir);
//
//	return result;
//}
