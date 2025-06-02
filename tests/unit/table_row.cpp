#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <vector>
#include "access.h"

struct MockHeapFile {
    std::vector<std::vector<std::byte>> rows;
    RID append(const std::vector<std::byte>& bytes) {
        rows.push_back(bytes);
        return RID{rows.size() - 1}; // Or whatever your RID is
    }
    std::vector<std::vector<std::byte>> full_scan() const {
        return rows;
    }
};


class AccessTest : public testing::Test {
protected:
    void SetUp() override {
        test_dir = std::filesystem::temp_directory_path() / "smoldb_test";
        std::filesystem::create_directories(test_dir);
        heap_ = std::make_unique<MockHeapFile>();
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir);
    }

    std::filesystem::path test_dir;
    std::unique_ptr<MockHeapFile> heap_;
};

TEST_F(AccessTest, BasicSchemaAndCatalog) {
    // Test original functionality
    Catalog cat(test_dir);
    Schema sch;
    Column c;
    c.id = 42;
    c.name = "foo";
    c.type = Col_type::INT;
    c.nullable = true;
    c.default_bytes = { std::byte{0x10}, std::byte{0x20} };
    sch.push_back(c);
    cat.register_schema(1, std::move(sch));

    auto p1 = test_dir / "catalog1.bin";
    auto p2 = test_dir / "catalog2.bin";

    cat.dump(p1);
    Catalog loaded(test_dir);
    loaded.load(p1);
    loaded.dump(p2);

    std::ifstream f1(p1, std::ios::binary), f2(p2, std::ios::binary);
    std::vector<char> b1{ std::istreambuf_iterator(f1), {} },
                      b2{ std::istreambuf_iterator(f2), {} };
    ASSERT_EQ(b1, b2);
}