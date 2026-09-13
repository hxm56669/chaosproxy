#include "phototask/filesystem_store.h"
#include "phototask/model/model.h"
#include "phototask/mysql_store.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace {

TEST(B1ModelTest, NormalizationIsCanonicalAndRejectsUnknownOperation) {
  phototask::TaskSpec spec{" request-1 ", "photo-1", {"EXTRACT_METADATA", "thumbnail", "thumbnail"}, 320, 240};
  const auto normalized = phototask::NormalizeTaskSpec(spec);
  ASSERT_TRUE(normalized.ok()) << normalized.status().message();
  EXPECT_EQ(normalized.value().spec.operations,
            (std::vector<std::string>{"extract_metadata", "thumbnail"}));
  EXPECT_EQ(normalized.value().fingerprint, phototask::FingerprintV1(normalized.value()));

  spec.operations = {"rotate"};
  EXPECT_FALSE(phototask::NormalizeTaskSpec(spec).ok());
}

TEST(B1ModelTest, StateMachineDoesNotAllowStaleCompletion) {
  EXPECT_TRUE(phototask::IsValidTransition(phototask::TaskState::kRunning,
                                            phototask::TaskState::kSucceeded));
  EXPECT_FALSE(phototask::IsValidTransition(phototask::TaskState::kReady,
                                             phototask::TaskState::kSucceeded));
}

TEST(B1FilesystemTest, ImportsContentAddressedFileAndRejectsTraversal) {
  const auto root = std::filesystem::temp_directory_path() / "phototask-b1-store";
  std::filesystem::remove_all(root);
  const auto input = root.parent_path() / "phototask-b1-input.ppm";
  {
    std::ofstream file(input, std::ios::binary);
    file << "P6\n2 1\n255\n" << '\xff' << '\0' << '\0' << '\0' << '\xff' << '\0';
  }
  phototask::FileSystemStore store(root);
  const auto imported = store.Import(input);
  ASSERT_TRUE(imported.ok()) << imported.status().message();
  EXPECT_EQ(imported.value().bytes, 17U);
  const auto content = store.Read(imported.value().original_path);
  ASSERT_TRUE(content.ok());
  EXPECT_EQ(content.value().size(), imported.value().bytes);
  EXPECT_FALSE(store.Read("../phototask-b1-input.ppm").ok());
  std::filesystem::remove(input);
  std::filesystem::remove_all(root);
}

TEST(B1MysqlTest, MigrationAndDuplicateImportAreIdempotent) {
  phototask::MysqlStore store;
  const auto migration = store.InitializeSchema(std::string(PHOTOTASK_SOURCE_DIR) + "/deploy/sql/001_core.sql");
  ASSERT_TRUE(migration.ok()) << migration.message();
  phototask::PhotoAsset asset{"photo-b1", "originals/b1.ppm", std::string(64, 'a'), 17, 2, 1, "{}"};
  ASSERT_TRUE(store.InsertImported(asset).ok());
  ASSERT_TRUE(store.InsertImported(asset).ok());
  const auto found = store.FindPhotoBySha256(asset.sha256);
  ASSERT_TRUE(found.ok()) << found.status().message();
  EXPECT_EQ(found.value().id, asset.id);
}

}  // namespace
