#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <cinttypes>

#include <memory>
#include <unordered_map>

#include "db/arena_wrapped_db_iter.h"
#include "db/db_iter.h"
#include "rocksdb/env.h"

#include "blob_file_reader.h"
#include "blob_format.h"
#include "blob_storage.h"
#include "titan_logging.h"
#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

class UnorderedIterator : public Iterator {
 public:
  UnorderedIterator(const TitanReadOptions &options, BlobStorage *storage,
                  std::shared_ptr<ManagedSnapshot> snap,
                  std::unique_ptr<ArenaWrappedDBIter> iter, SystemClock *clock,
                  TitanStats *stats, Logger *info_log,Slice* lowerbound,Slice * upperbound,const Comparator* cmp)
      : options_(options),
        storage_(storage),
        snap_(snap),
        iter_(std::move(iter)),
        clock_(clock),
        stats_(stats),
        info_log_(info_log),
        lower_key_(lowerbound),
        upper_key_(upperbound),
        user_comparator_(cmp) {start();}

  ~UnorderedIterator() {
    RecordInHistogram(statistics(stats_), TITAN_ITER_TOUCH_BLOB_FILE_COUNT,
                      touched_file_nums);
  }

  bool Valid() const override { return mode!=StopMode && status_.ok(); }

  Status status() const override {
    // assume volatile inner iter
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void start(){
    if(lower_key_){
      iter_->Seek(*lower_key_);
    }
    else iter_->SeekToFirst();
    first_one=true;
    Next();
  }

  void SeekToFirst() override {
    assert(0);
  }

  void SeekToLast() override {
    assert(0);
  }

  void Seek(const Slice &target) override {
    assert(0);
  }

  void SeekForPrev(const Slice &target) override {
    assert(0);
  }

  void Next() override {
    assert(Valid());
    if(mode==Mode::LSMTreeMode){
      if(first_one)first_one=false;
      else iter_->Next();
      for(;
        iter_->Valid()&&memory_usage<max_unorder_iter_memory_usage_&&!keyGEthanRequire();
        memory_usage+=2*sizeof(uint64_t),iter_->Next()){
        Slice val=iter_->value();

        if (!iter_->IsBlob()){
          now_key_=iter_->key();
          now_value_=iter_->value();
          return;
        }

        BlobIndex index;
        status_=DecodeInto(val, &index);
        if(!status_.ok()){
        return;
      }
        blob_map[index.file_number].push_back(index.blob_handle);
      }

      if(blob_map.empty()){
        mode=Mode::StopMode;
        return;        
      }
      mode=Mode::BlobMode;
      blob_map_iter=blob_map.begin();
      status_=switchToNewBlob();
      if(!status_.ok()){
        return;
      }
    }
    status_=GetBlobValue();
    if(!status_.ok()){
      return;
    }
    now_key_=record_.key;
    now_value_=record_.value;
    if(++blob_iter==blob_map_iter->second.end()){
      if(++blob_map_iter==blob_map.end()){
        //all blob tasks were finished
        blob_map.clear();
        if(iter_->Valid())mode=Mode::LSMTreeMode;
        else mode=Mode::StopMode;
      }
      else{
        status_=switchToNewBlob();
        if(!status_.ok()){
          return;
        }
      }
    }
  }

  void Prev() override {
    assert(0);
  }

  Slice key() const override {
    assert(Valid());
    return now_key_;
  }

  Slice value() const override {
    assert(Valid());
    return now_value_;
  }

 private:

  enum Mode{
    LSMTreeMode,
    BlobMode,
    StopMode
  };

  static bool CompareBlobHandles(const BlobHandle& lhs, const BlobHandle& rhs) {
    if (lhs.offset != rhs.offset) {
      return lhs.offset < rhs.offset;
    }
    return lhs.size < rhs.size;
  }

  bool keyGEthanRequire(){
    if(!iter_->Valid())return true;
    else if(!upper_key_)return false;
    else return(user_comparator_->Compare(iter_->key(),*upper_key_)>=0);
  }

  //require: blob_map_iter has been switched to new blob
  Status switchToNewBlob(){
    std::sort(blob_map_iter->second.begin(), blob_map_iter->second.end(), CompareBlobHandles);
    //NewPrefetcher will free old prefetcher
    Status s=storage_->NewPrefetcher(blob_map_iter->first, &now_blob_prefetcher_);
    if(s.ok()){
      touched_file_nums++;
      blob_iter=blob_map_iter->second.begin();
    }
    return s;
  }
  
  Status GetBlobValue() {
    assert(iter_->status().ok());

    Status s;
    BlobIndex index;
    index.file_number=blob_map_iter->first;
    index.blob_handle=*blob_iter;

    std::string cache_key;
    auto blob_cache = storage_->blob_cache();
    if (blob_cache) {
      cache_key = storage_->EncodeBlobCache(index);
      bool cache_hit;
      s = storage_->TryGetBlobCache(cache_key, &record_, &buffer_, &cache_hit);
      if (!s.ok()) return s;
      if (cache_hit) return s;
    }


    buffer_.Reset();
    OwnedSlice blob;
    s = now_blob_prefetcher_->Get(options_, index.blob_handle, &record_, &blob);
    if (!s.ok()) {
      TITAN_LOG_ERROR(
          info_log_,
          "Titan iterator: failed to read blob value from file %" PRIu64
          ", offset %" PRIu64 ", size %" PRIu64 ": %s\n",
          index.file_number, index.blob_handle.offset, index.blob_handle.size,
          s.ToString().c_str());
      if (options_.abort_on_failure) std::abort();
    }

    if (blob_cache && options_.fill_cache) {
      Cache::Handle *cache_handle = nullptr;
      auto cache_value = new OwnedSlice(std::move(blob));
      blob_cache->Insert(cache_key, cache_value, &kBlobValueCacheItemHelper,
                         cache_value->size() + sizeof(*cache_value),
                         &cache_handle, Cache::Priority::BOTTOM);
      buffer_.PinSlice(*cache_value, UnrefCacheHandle, blob_cache,
                       cache_handle);
    } else {
      buffer_.PinSlice(blob, OwnedSlice::CleanupFunc, blob.release(), nullptr);
    }
    return s;
  }

  Status status_;
  BlobRecord record_;
  PinnableSlice buffer_;
  TickerType type_;

  TitanReadOptions options_;
  BlobStorage *storage_;
  std::shared_ptr<ManagedSnapshot> snap_;
  std::unique_ptr<ArenaWrappedDBIter> iter_;

  SystemClock *clock_;
  TitanStats *stats_;
  Logger *info_log_;

  Slice* lower_key_;
  Slice* upper_key_;
  const Comparator* user_comparator_;
  std::unique_ptr<BlobFilePrefetcher> now_blob_prefetcher_=nullptr;
  uint64_t touched_file_nums=0;
  
  Slice now_key_;
  Slice now_value_;

  Mode mode;
  std::unordered_map<uint64_t,std::vector<BlobHandle>> blob_map;
  std::unordered_map<uint64_t,std::vector<BlobHandle>>::iterator blob_map_iter;
  std::vector<BlobHandle>::iterator blob_iter;
  uint64_t memory_usage=0;
  uint64_t max_unorder_iter_memory_usage_=512<<20;
  bool first_one=true;
  
};

}  // namespace titandb
}  // namespace rocksdb
