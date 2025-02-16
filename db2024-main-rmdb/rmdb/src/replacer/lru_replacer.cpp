/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;  

/**
 * @description: 使用LRU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool LRUReplacer::victim(frame_id_t* frame_id) {
    // C++17 std::scoped_lock
    // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
    std::scoped_lock lock{latch_};  // 如果编译报错可以替换成其他锁类型

    // Todo:
    // 利用LRUlist_和LRUHash_实现LRU策略
    // 选择合适的frame作为淘汰页面，并将其赋值给*frame_id

    if (LRUlist_.empty()) {
        // 队列为空，没有可以淘汰的页面
        frame_id= nullptr;
        // std::scoped_lock unlock{latch_};
        return false;
    }

    // 从尾部开始向前遍历
    for (auto it = LRUlist_.rbegin(); it != LRUlist_.rend(); ++it) {
        if (LRUhash_.find(*it) != LRUhash_.end()) {
            // 是可以淘汰的页面
            *frame_id = *it;
            LRUhash_.erase(*it);
            LRUlist_.remove(*it);
            return true;
        }
    }

    // 所有页面都不可淘汰
    frame_id= nullptr;
    return false;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    // Todo:
    // 固定指定id的frame
    // 在数据结构中移除该frame

    // 查找要固定的页面是否存在于LRU替换器中
    auto it = LRUhash_.find(frame_id);
    if (it != LRUhash_.end()) {
        // 如果存在，从LRU哈希表和LRU列表中移除该页面
        LRUhash_.erase(it);
        LRUlist_.erase(it->second);
    }

    // std::scoped_lock unlock{latch_};
}
/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    // Todo:
    // 支持并发锁
    std::scoped_lock lock{latch_};
    // 选择一个frame取消固定

    // 如果LRU替换器中不存在该frame并且当前大小小于最大容量
    if (LRUhash_.find(frame_id) == LRUhash_.end() && Size() < max_size_) {
        // 将frame_id放入LRU列表头部
        LRUlist_.push_front(frame_id);
        // 更新LRU哈希表中frame_id的位置
        LRUhash_[frame_id] = LRUlist_.begin();
    }

    // std::scoped_lock unlock{latch_};
    // std::cout<<"unpin: "<<frame_id<<std::endl;
}
/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
