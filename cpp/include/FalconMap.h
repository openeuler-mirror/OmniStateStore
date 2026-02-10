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
        svuint8_t a = svld1_u8(pg, p);
        svuint8_t b = svld1_u8(pg, q);

        // 比较并检查差异
        svbool_t diff = svcmpne(pg, a, b);
        if (svptest_any(svptrue_b8(), diff)) {
            return false;
        }

        size_t vl = svcntb();
        p += vl;
        q += vl;
        len -= vl;
    }
    return true;
}

static inline uint64_t sve_extract_u64(svuint64_t vec, size_t idx) noexcept {
    uint64_t result = 0;
    // 内联汇编：将向量vec的第idx个64位元素（.d表示64位双字）赋值给result
    // %0: 输出变量result，%1: 输入向量vec（w表示SVE向量寄存器），%2: 输入索引idx
    __asm__ __volatile__(
        "st1d %1.d, p0, [%3]\n"        // 将整个向量存到栈上
        "ldr %0, [%3, %2, lsl #3]\n"   // 从偏移处读取第 idx 个元素
        : "=r"(result)
        : "w"(vec), "r"(idx), "r"(&result)  // 需要临时栈空间
        : "memory", "p0"
    );
    return result;
}

static inline uint32_t do_crc_for_valid_bytes(uint32_t crc, size_t valid_bytes, uint64_t qword)
{
    switch (valid_bytes) { // 按有效字节选择对应CRC指令，避免零填充影响结果
        case 8:
            crc = __builtin_aarch64_crc32cx(crc, qword);
            break;
        case 7:
            crc = __builtin_aarch64_crc32cw(crc, static_cast<uint32_t>(qword >> 32));
            crc = __builtin_aarch64_crc32ch(crc, static_cast<uint16_t>(qword >> 16));
            crc = __builtin_aarch64_crc32cb(crc, static_cast<uint8_t>(qword));
            break;
        case 6:
            crc = __builtin_aarch64_crc32cw(crc, static_cast<uint32_t>(qword >> 32));
            crc = __builtin_aarch64_crc32ch(crc, static_cast<uint16_t>(qword >> 16));
            break;
        case 5:
            crc = __builtin_aarch64_crc32cw(crc, static_cast<uint32_t>(qword >> 32));
            crc = __builtin_aarch64_crc32cb(crc, static_cast<uint8_t>(qword >> 16));
            break;
        case 4:
            crc = __builtin_aarch64_crc32cw(crc, static_cast<uint32_t>(qword));
            break;
        case 3:
            crc = __builtin_aarch64_crc32ch(crc, static_cast<uint16_t>(qword));
            crc = __builtin_aarch64_crc32cb(crc, static_cast<uint8_t>(qword >> 16));
            break;
        case 2:
            crc = __builtin_aarch64_crc32ch(crc, static_cast<uint16_t>(qword));
            break;
        case 1:
            crc = __builtin_aarch64_crc32cb(crc, static_cast<uint8_t>(qword));
            break;
        default:
            break;
    }
    return crc;
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

        const size_t vl = svcntb();  // 向量长度（字节）：16/32/64
        const size_t vl_u64 = vl / 8; // 64-bit 元素个数

        // 初始化 SVE 状态向量
        svuint64_t state = svdup_u64(0x1234567890abcdefULL);
        svuint64_t mixer = svdup_u64(0x9e3779b97f4a7c15ULL);
        uint64_t seed = len;

        // 对齐前导字节
        size_t offset = 0;
        while (((uintptr_t)(data + offset) & 7) && offset < len) {
            seed = __builtin_aarch64_crc32cb(seed, data[offset]);
            ++offset;
        }

        const uint64_t* ptr = reinterpret_cast<const uint64_t*>(data + offset);
        size_t remaining = len - offset;

        // SVE 主循环：每次处理 vl 字节
        // 使用 WHILELT 确保不越界
        size_t i = 0;
        while (i + vl <= remaining) {
            svbool_t pg = svwhilelt_b8_u64(i, remaining);

            // 加载 vl 字节
            svuint8_t bytes = svld1_u8(pg, data + offset + i);
            svuint64_t vals = svreinterpret_u64_u8(bytes);

            // 混合：state = (state ^ vals) * mixer
            state = sveor_u64_z(pg, state, vals);
            state = svmul_u64_z(pg, state, mixer);

            // 旋转 mixer 保持熵
            mixer = svadd_u64_z(pg, mixer, svdup_u64(0x9e3779b97f4a7c15ULL));
            i += vl;
        }

        // 规约 SVE 向量到标量
        uint64_t hash = svlasta_u64(svptrue_b64(), state) ^ seed;

        // 处理剩余字节（标量）
        const uint8_t* tail = data + offset + i;
        remaining -= i;

        while (remaining >= 8) {
            hash = mix(hash, *reinterpret_cast<const uint64_t*>(tail));
            tail += 8;
            remaining -= 8;
        }

        // 最后 <8 字节
        if (remaining > 0) {
            uint64_t last = 0;
            memcpy(&last, tail, remaining);
            hash = mix(hash, last ^ (remaining << 56));
        }

        // 最终化
        hash = mix(hash, hash >> 32);
        hash = mix(hash, hash >> 16);
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
