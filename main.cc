#include <stdio.h>

#include <vector>

#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

using namespace std;
using namespace rocksdb;

class CustomCompactionFilter : public CompactionFilter {
  ~CustomCompactionFilter() {}

  const char *Name() const { return "CustomCompactionFilter"; }

  bool Filter(int level, const Slice &key, const Slice &existing_value,
              std::string *new_value, bool *value_changed) const {
    char *buf = (char *)malloc(key.size() + 1);
    bzero(buf, key.size() + 1);
    memcpy(buf, key.data(), key.size());
    printf("filter %s is called\n", buf);
    free(buf);
    return false;
  }
};

class CustomCompactionFilterFactory : public CompactionFilterFactory {
  ~CustomCompactionFilterFactory() {}

  unique_ptr<CompactionFilter>
  CreateCompactionFilter(const CompactionFilter::Context &context) {
    if (context.is_manual_compaction) {
      printf("create custom filter is called for manual compaction\n");
      CustomCompactionFilter *filter = new CustomCompactionFilter();
      return unique_ptr<CompactionFilter>(filter);
    }
    printf("create custom filter is called for auto compaction\n");
    return unique_ptr<CompactionFilter>(nullptr);
  }

  const char *Name() const { return "custom_compaction_filter_factory"; }
};

int main() {
  Options options;
  options.create_if_missing = true;

  DB *db;
  assert(DB::Open(options, "/tmp/qupeng-rocksdb", &db).ok());

  ColumnFamilyHandle *cf;
  assert(db->CreateColumnFamily(ColumnFamilyOptions(), "new_cf", &cf).ok());
  delete cf; // After this, the column family still exists.
  delete db;

  // Open column families.
  ColumnFamilyOptions cf_options;
  CompactionFilterFactory *factory = new CustomCompactionFilterFactory();
  cf_options.compaction_filter_factory.reset(factory);

  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  column_families.push_back(ColumnFamilyDescriptor("new_cf", cf_options));

  std::vector<ColumnFamilyHandle *> handles;
  assert(
      DB::Open(options, "/tmp/qupeng-rocksdb", column_families, &handles, &db)
          .ok());

  for (int i = 0; i < 10000; ++i) {
    char buff[100];
    bzero(buff, 100);
    sprintf(buff, "key%d", i % 100);
    Slice key = Slice(buff);
    assert(db->Put(WriteOptions(), handles[1], key, Slice("value")).ok());

    std::string value;
    db->Get(ReadOptions(), handles[1], key, &value);
    printf("value: %s\n", value.c_str());

    if (i % 100 == 99) {
      db->Flush(FlushOptions(), handles[1]);
    }
  }

  CompactRangeOptions compact_opts;
  compact_opts.allow_write_stall = false;
  Slice begin = Slice("k");
  Slice end = Slice("l");
  assert(db->CompactRange(compact_opts, handles[1], &begin, &end).ok());

  for (unsigned int i = 0; i < handles.size(); ++i) {
    delete handles[i];
  }
  delete db;

  return 0;
}
