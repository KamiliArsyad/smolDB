#include <filesystem>
#include <iostream>

#include "access/access.h"
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

    // --- Interaction with the DB ---

    const std::string table_name = "users";
    Table<>* users_table = db.get_table(table_name);

    if (users_table == nullptr)
    {
      std::cout << "Table '" << table_name << "' not found. Creating it."
                << std::endl;
      Schema schema = make_simple_schema();
      db.create_table(1, table_name, schema);
      users_table = db.get_table(table_name);

      Row row(schema);
      row.set_value("id", 101);
      row.set_value("name", "Alice");
      users_table->insert_row(row);

      Row row2(schema);
      row2.set_value("id", 102);
      row2.set_value("name", "Bob");
      users_table->insert_row(row2);
      std::cout << "Inserted 2 rows into new table." << std::endl;
    }
    else
    {
      std::cout << "Table '" << table_name << "' found." << std::endl;
    }

    // Scan and print all rows
    std::cout << "Scanning all rows from '" << table_name << "':" << std::endl;
    auto all_rows = users_table->scan_all();
    for (const auto& row : all_rows)
    {
      int32_t id = boost::get<int32_t>(row.get_value("id"));
      std::string name = boost::get<std::string>(row.get_value("name"));
      std::cout << "  - ID: " << id << ", Name: " << name << std::endl;
    }
    std::cout << "Found " << all_rows.size() << " rows." << std::endl;

    std::cout << "Shutting down SmolDB." << std::endl;
    // db.shutdown() will be called by the destructor
  }

  std::cout << "DB shutdown complete." << std::endl;

  return 0;
}