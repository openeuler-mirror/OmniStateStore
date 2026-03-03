#ifndef FALCONMAP_H
#define FALCONMAP_H

#include <arm_sve.h>
#include <unordered_map>
#include <arm_neon.h>
#include <arm_acle.h>
#include <string>
#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <iostream>

#if !defined(__ARM_FEATURE_SVE)
    #error "This code requires ARM SVE support"
#endif

static inline uint8_t check_equal(const uint8_t* p, const uint8_t* q, uint32_t len)
{
    while (len > 0) {
        svbool_t pg = svwhilelt_b8_u32(0, len);
        svuint8_t a = svld1_u8(pg, p); // 从内存加载数据到 SVE 向量寄存器
        svuint8_t b = svld1_u8(pg, q);
        svbool_t diff = svcmpne(pg, a, b); // 比较两个向量是否不相等
        if (svptest_any(svptrue_b8(), diff)) {
            return false; // true说明有不匹配，返回false
        }

        size_t sve_vec_bytes = svcntb();
        p += sve_vec_bytes;
        q += sve_vec_bytes;
        len -= sve_vec_bytes;
    }
    return true;
}

// 工具：混合两个 64-bit 值（基于 SplitMix64）
inline uint64_t mix(uint64_t x, uint64_t y) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31) ^ y;
}

template <typename K>
struct FalconHash {
    std::size_t operator()(const K& k) const {
        const uint8_t* data = reinterpret_cast<const uint8_t*>(k.data());
        size_t len = k.size();
        if (len == 0 || data == nullptr) {
            return 0;
        }

        const size_t sve_vec_bytes = svcntd();  // 向量长度（字节）：16/32/64
        // 初始化 SVE 状态向量
        svuint64_t state = svdup_u64(0x1234567890abcdefULL);
        svuint64_t mixer = svdup_u64(0x9e3779b97f4a7c15ULL); // 乘法常数，用于扩散比特，类似 Fibonacci hashing
        uint64_t seed = len;

         // 强制 8 字节对齐指针（关键优化）
        const uint64_t* ptr64 = reinterpret_cast<const uint64_t*>((reinterpret_cast<uintptr_t>(data) + 7) & ~7);
        __builtin_prefetch(ptr64, 0, 3); // 预取当前块到L1，可读写，高优先级
        // 处理前导未对齐字节（使后续 64-bit 加载对齐）
        size_t offset = reinterpret_cast<const uint8_t*>(ptr64) - data;
        uint64_t hash = seed;
        for (size_t i = 0; i < offset && i < len; ++i) {
            hash = __builtin_aarch64_crc32cb(hash, data[i]);
        }

        size_t remaining = (len > offset) ? (len - offset) : 0;
        size_t num_qwords = remaining / 8;  // 64-bit 块数

        // SVE 主循环：明确使用 64-bit 元素计数
        while (num_qwords >= sve_vec_bytes) {
            svbool_t pg = svptrue_b64();  // 全谓词，明确 64-bit
            svuint64_t vals = svld1_u64(pg, ptr64);
            // 混合
            state = sveor_u64_x(pg, state, vals);
            state = svmul_u64_x(pg, state, mixer);
            // 更新 mixer（标量方式更高效）
            uint64_t m = svaddv_u64(pg, mixer);
            mixer = svdup_u64(m + 0x9e3779b97f4a7c15ULL);

            ptr64 += sve_vec_bytes;
            num_qwords -= sve_vec_bytes;
        }

        // 合并状态到 hash
        hash ^= svaddv_u64(svptrue_b64(), state);

        // 处理剩余 64-bit 块（标量）
        const uint8_t* tail = reinterpret_cast<const uint8_t*>(ptr64);
        while (num_qwords > 0) {
            hash = mix(hash, *ptr64++);
            --num_qwords;
            tail += 8;
        }

        // 最后 <8 字节
        remaining = remaining % 8;
        for (size_t i = 0; i < remaining; ++i) {
            hash = __builtin_aarch64_crc32cb(hash, tail[i]);
        }

        // 最终化
        hash = mix(hash, hash >> 32);
        return static_cast<std::size_t>(hash);
    }
};

template <typename K1, typename K2>
struct FalconEqual {
    bool operator()(const K1 &a, const K2 &b) const {
        if (a.size() != b.size()) {
            return false;
        }

        //std::cout << "a : " << a.data() << ", b : " << b.data() << std::endl;
        const uint8_t* p = reinterpret_cast<const uint8_t*>(a.data());
        const uint8_t* q = reinterpret_cast<const uint8_t*>(b.data());
        uint32_t len = a.size();
        const uint32_t sve_vec_bytes = svcntb(); // 每个SVE向量的字节数（如16/32/64）

        while (len >= sve_vec_bytes) {
            // 汇编实现：SVE加载+比较+检测不等（替代svld1b_z/svcmpeq_u8/svptest_any）
            if (!check_equal(p, q, sve_vec_bytes)) {
                //std::cout << "key not equal, sve_vec_bytes : " << sve_vec_bytes << std::endl;
                return false;
            }
            p += sve_vec_bytes;
            q += sve_vec_bytes;
            len -= sve_vec_bytes;
        }

        bool result = memcmp(p, q, len);
        //std::cout << "key equal result: " << result << std::endl;
        return (result == 0);
    }
};

#endif
