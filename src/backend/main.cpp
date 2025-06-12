#include <filesystem>
#include <iostream>

#include "access/access.h"
#include "executor/trx.h"
#include "smoldb.h"

// Helper to create a simple schema for demonstration
static Schema make_simple_schema()
{
  Column c0{0, "id", Col_type::INT, false, {}};
  Column c1{1, "name", Col_type::STRING, false, {}};
  return Schema{c0, c1};
}

int main(int argc, char** argv)
{
  std::filesystem::path db_dir = "./smoldb_data";
  std::cout << "Starting SmolDB instance in: " << db_dir.string() << std::endl;

  // Create and startup the database using RAII
  {
    SmolDB db(db_dir);
    db.startup();

    const std::string table_name = "users";
    Table<>* users_table = db.get_table(table_name);

    // --- Transactional Interaction with the DB ---
    TransactionID txn = db.begin_transaction();

    if (users_table == nullptr)
    {
      std::cout << "Table '" << table_name << "' not found. Creating it."
                << std::endl;
      Schema schema = make_simple_schema();
      db.create_table(1, table_name, schema);
    }
    else
    {
      std::cout << "Table '" << table_name << "' found." << std::endl;
    }

    // Re-get table object after potential creation
    users_table = db.get_table(table_name);

    // Insert a new row in the current transaction
    try
    {
      Schema schema = make_simple_schema();
      Row row(schema);
      row.set_value("id", 201);
      row.set_value("name", "Charlie");
      users_table->insert_row(txn, row);
      std::cout << "Inserted a row into '" << table_name << "'." << std::endl;

      // Scan and print all rows within the same transaction
      std::cout << "Scanning all rows from '" << table_name << "':" << std::endl;
      auto all_rows = users_table->scan_all(); // Note: Scan is not transactional yet
      for (const auto& row : all_rows)
      {
        int32_t id = boost::get<int32_t>(row.get_value("id"));
        std::string name = boost::get<std::string>(row.get_value("name"));
        std::cout << "  - ID: " << id << ", Name: " << name << std::endl;
      }

      std::cout << "Committing transaction " << txn << std::endl;
      db.commit_transaction(txn);
    }
    catch (const std::exception& e)
    {
      std::cerr << "Transaction failed: " << e.what() << std::endl;
      std::cout << "Aborting transaction " << txn << std::endl;
      db.abort_transaction(txn);
    }

    std::cout << "Shutting down SmolDB." << std::endl;
    // db.shutdown() will be called by the destructor
  }

  std::cout << "DB shutdown complete." << std::endl;

  return 0;
}