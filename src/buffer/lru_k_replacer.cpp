//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {

}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    

    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    if(node_store_.find(frame_id) == end(node_store_)) {
        if(frame_id < 0 || frame_id >= replacer_size_) {
            throw Exception("frame id is not valid!!");
        }
        node_store_[frame_id] = LRUKNode();
    }
    LRUKNode frame = node_store_[frame_id];
    frame.RecordAccess(GetCurrentTime());
    IncrementCurrentTime();


}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if(node_store_.find(frame_id) == end(node_store_)) {
        throw Exception("frame id is not valid!!");
    }
    LRUKNode frame = node_store_[frame_id];
    bool prev_evict_state = frame.IsEvictable();
    if(prev_evict_state) {
        if(!set_evictable) {
            frame.SetEvictable(false);
            curr_size_--;
        }
    } else {
        if(set_evictable) {
            frame.SetEvictable(true);
            curr_size_++;
        }
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    if(node_store_.find(frame_id) == end(node_store_)) {
        return;
    }
    LRUKNode frame = node_store_[frame_id];
    if(frame.IsEvictable()) {
        throw Exception("frame is not evictable!!");
    }
    node_store_.erase(frame_id);
    curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

auto LRUKReplacer::GetCurrentTime() -> size_t { return current_timestamp_; }

void LRUKReplacer::IncrementCurrentTime() { current_timestamp_++; }

auto LRUKReplacer::GetK() -> size_t { return k_; }

}  // namespace bustub
