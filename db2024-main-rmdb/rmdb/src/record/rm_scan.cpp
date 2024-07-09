/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // 初始化
    // 初始化从第一个页开始扫描
    rid_.page_no = RM_FIRST_RECORD_PAGE;
    rid_.slot_no = -1;  // 初始化为-1，表示还没有找到有效记录
    next();  // 查找第一个有效记录
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // 查找文件中下一个存放记录的非空闲位置，并使用rid_指向该位置

    // 循环遍历页号直到文件中的所有页都被检查过
    while (rid_.page_no < file_handle_->file_hdr_.num_pages) {
        // 获取当前页的页面句柄
        RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);

        // 在当前页中循环遍历槽位，直到找到一个有效记录
        while (rid_.slot_no < file_handle_->file_hdr_.num_records_per_page - 1) {
            rid_.slot_no++;
            if (Bitmap::is_set(page_handle.bitmap, rid_.slot_no)) {
                return;  // 找到一个有效记录
            }
        }

        // 当前页的所有槽位都检查完毕，移动到下一页
        rid_.page_no++;
        rid_.slot_no = -1;  // 重置slot_no以开始检查下一页
    }
}
/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // 判断扫描是否结束

    // 如果当前页号大于等于文件中的总页数，则扫描结束
    if (rid_.page_no >= file_handle_->file_hdr_.num_pages) {
        return true;
    }

    // 如果当前槽号大于等于当前页中记录的总数，则扫描结束
    if (rid_.slot_no >= file_handle_->fetch_page_handle(rid_.page_no).page_hdr->num_records) {
        return true;
    }

    // 扫描未结束
    return false;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}