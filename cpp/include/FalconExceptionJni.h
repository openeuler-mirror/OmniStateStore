/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#ifndef FALCON_FALCONEXCEPTIONJNI_H
#define FALCON_FALCONEXCEPTIONJNI_H

#include <jni.h>
#include <cassert>
#include <string>
#include <iostream>
#include <memory>
#include "rocksdb/status.h"

namespace FalconException {

/** JavaClass is copied from rocksdb project: java/rocksjni/portal.h */
class JavaClass {
public:
    /**
     * Gets and initializes a Java Class
     *
     * @param env A pointer to the Java environment
     * @param jclazz_name The fully qualified JNI name of the Java Class
     *     e.g. "java/lang/String"
     *
     * @return The Java Class or nullptr if one of the
     *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
     *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
     */
    static jclass getJClass(JNIEnv *env, const char *jclazz_name)
    {
        jclass jclazz = env->FindClass(jclazz_name);
        assert(jclazz != nullptr);
        return jclazz;
    }
};

/** JavaException is copied from rocksdb project: /java/rocksjni/portal.h */
template<class DERIVED>
class JavaException : public JavaClass {
public:
    /**
     * Create and throw a java exception with the provided message
     *
     * @param env A pointer to the Java environment
     * @param msg The message for the exception
     *
     * @return true if an exception was thrown, false otherwise
     */
    static bool ThrowNew(JNIEnv *env, const std::string &msg)
    {
        jclass jclazz = DERIVED::getJClass(env);
        if (jclazz == nullptr) {
            // exception occurred accessing class
            std::cerr << "JavaException::ThrowNew - Error: unexpected exception!" << std::endl;
            return env->ExceptionCheck();
        }

        const jint rs = env->ThrowNew(jclazz, msg.c_str());
        if (rs != JNI_OK) {
            // exception could not be thrown
            std::cerr << "JavaException::ThrowNew - Fatal: could not throw exception!" << std::endl;
            return env->ExceptionCheck();
        }

        return true;
    }
};

/** Native class template, which is copied from rocksdb project: /java/rocksjni/portal.h */
template<class PTR, class DERIVED> class RocksDBNativeClass : public JavaClass {
};

/** SubCodeJni is copied from rocksdb project: /java/rocksjni/portal.h */
class SubCodeJni : public JavaClass {
public:
    /**
     * Get the Java Class org.rocksdb.Status.SubCode
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Class or nullptr if one of the ClassFormatError, ClassCircularityError, NoClassDefFoundError,
     *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
     */
    static jclass getJClass(JNIEnv* env)
    {
        return JavaClass::getJClass(env, "org/rocksdb/Status$SubCode");
    }

    /**
     * Get the Java Method: Status.SubCode#getValue
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Method ID or nullptr if the class or method id could not be retrieved
     */
    static jmethodID getValueMethod(JNIEnv* env)
    {
        jclass jclazz = getJClass(env);
        if (jclazz == nullptr) {
            // exception occurred accessing class
            return nullptr;
        }

        static jmethodID mid = env->GetMethodID(jclazz, "getValue", "()b");
        assert(mid != nullptr);
        return mid;
    }

    static ROCKSDB_NAMESPACE::Status::SubCode toCppSubCode(const jbyte jsub_code)
    {
        switch (jsub_code) {
            case 0x0:
                return ROCKSDB_NAMESPACE::Status::SubCode::kNone;
            case 0x1:
                return ROCKSDB_NAMESPACE::Status::SubCode::kMutexTimeout;
            case 0x2:
                return ROCKSDB_NAMESPACE::Status::SubCode::kLockTimeout;
            case 0x3:
                return ROCKSDB_NAMESPACE::Status::SubCode::kLockLimit;
            case 0x4:
                return ROCKSDB_NAMESPACE::Status::SubCode::kNoSpace;
            case 0x5:
                return ROCKSDB_NAMESPACE::Status::SubCode::kDeadlock;
            case 0x6:
                return ROCKSDB_NAMESPACE::Status::SubCode::kStaleFile;
            case 0x7:
                return ROCKSDB_NAMESPACE::Status::SubCode::kMemoryLimit;
            case 0x7F:
            default:
                return ROCKSDB_NAMESPACE::Status::SubCode::kNone;
        }
    }
};

/** CodeJni is copied from rocksdb project: /java/rocksjni/portal.h */
class CodeJni : public JavaClass {
public:
    /**
     * Get the Java Class org.rocksdb.Status.Code
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Class or nullptr if one of the
     *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
     *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
     */
    static jclass getJClass(JNIEnv* env)
    {
        return JavaClass::getJClass(env, "org/rocksdb/Status$Code");
    }

    /**
     * Get the Java Method: Status.Code#getValue
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Method ID or nullptr if the class or method id could not
     *     be retrieved
     */
    static jmethodID getValueMethod(JNIEnv* env)
    {
        jclass jclazz = getJClass(env);
        if (jclazz == nullptr) {
            // exception occurred accessing class
            return nullptr;
        }

        static jmethodID mid = env->GetMethodID(jclazz, "getValue", "()b");
        assert(mid != nullptr);
        return mid;
    }
};

/** StatusJni is copied from rocksdb project: /java/rocksjni/portal.h */
class StatusJni : public RocksDBNativeClass<ROCKSDB_NAMESPACE::Status*, StatusJni> {
public:
    /**
     * Get the Java Class org.rocksdb.Status
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Class or nullptr if one of the ClassFormatError, ClassCircularityError, NoClassDefFoundError,
     *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
     */
    static jclass getJClass(JNIEnv* env)
    {
        return RocksDBNativeClass::getJClass(env, "org/rocksdb/Status");
    }

    /**
     * Get the Java Method: Status#getCode
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Method ID or nullptr if the class or method id could not be retrieved
     */
    static jmethodID getCodeMethod(JNIEnv* env)
    {
        jclass jclazz = getJClass(env);
        if (jclazz == nullptr) {
            // exception occurred accessing class
            return nullptr;
        }

        static jmethodID mid = env->GetMethodID(jclazz, "getCode", "()Lorg/rocksdb/Status$Code;");
        assert(mid != nullptr);
        return mid;
    }

    /**
     * Get the Java Method: Status#getSubCode
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Method ID or nullptr if the class or method id could not be retrieved
     */
    static jmethodID getSubCodeMethod(JNIEnv* env)
    {
        jclass jclazz = getJClass(env);
        if (jclazz == nullptr) {
            // exception occurred accessing class
            return nullptr;
        }

        static jmethodID mid = env->GetMethodID(jclazz, "getSubCode", "()Lorg/rocksdb/Status$SubCode;");
        assert(mid != nullptr);
        return mid;
    }

    /**
     * Get the Java Method: Status#getState
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Method ID or nullptr if the class or method id could not be retrieved
     */
    static jmethodID getStateMethod(JNIEnv* env)
    {
        jclass jclazz = getJClass(env);
        if (jclazz == nullptr) {
            // exception occurred accessing class
            return nullptr;
        }

        static jmethodID mid = env->GetMethodID(jclazz, "getState", "()Ljava/lang/String;");
        assert(mid != nullptr);
        return mid;
    }

    /**
     * Create a new Java org.rocksdb.Status object with the same properties as the provided C++
     * ROCKSDB_NAMESPACE::Status object
     *
     * @param env A pointer to the Java environment
     * @param status The ROCKSDB_NAMESPACE::Status object
     *
     * @return A reference to a Java org.rocksdb.Status object, or nullptr if an an exception occurs
     */
    static jobject construct(JNIEnv* env, const ROCKSDB_NAMESPACE::Status& status)
    {
        jclass jclazz = getJClass(env);
        if (jclazz == nullptr) {
            // exception occurred accessing class
            return nullptr;
        }

        jmethodID mid = env->GetMethodID(jclazz, "<init>", "(BBLjava/lang/String;)V");
        if (mid == nullptr) {
            // exception thrown: NoSuchMethodException or OutOfMemoryError
            return nullptr;
        }

        // convert the Status state for Java
        jstring jstate = nullptr;
        if (status.getState() != nullptr) {
            const char* const state = status.getState();
            jstate = env->NewStringUTF(state);
            if (env->ExceptionCheck()) {
                if (jstate != nullptr) {
                    env->DeleteLocalRef(jstate);
                }
                return nullptr;
            }
        }

        jobject jstatus = env->NewObject(jclazz, mid, toJavaStatusCode(status.code()),
                                         toJavaStatusSubCode(status.subcode()), jstate);
        if (env->ExceptionCheck()) {
            // exception occurred
            if (jstate != nullptr) {
                env->DeleteLocalRef(jstate);
            }
            return nullptr;
        }

        if (jstate != nullptr) {
            env->DeleteLocalRef(jstate);
        }

        return jstatus;
    }

    static jobject construct(JNIEnv* env, const ROCKSDB_NAMESPACE::Status* status)
    {
        return construct(env, *status);
    }

    // Returns the equivalent org.rocksdb.Status.Code for the provided
    // C++ ROCKSDB_NAMESPACE::Status::Code enum
    static jbyte toJavaStatusCode(const ROCKSDB_NAMESPACE::Status::Code& code)
    {
        switch (code) {
            case ROCKSDB_NAMESPACE::Status::Code::kOk:
                return 0x0;
            case ROCKSDB_NAMESPACE::Status::Code::kNotFound:
                return 0x1;
            case ROCKSDB_NAMESPACE::Status::Code::kCorruption:
                return 0x2;
            case ROCKSDB_NAMESPACE::Status::Code::kNotSupported:
                return 0x3;
            case ROCKSDB_NAMESPACE::Status::Code::kInvalidArgument:
                return 0x4;
            case ROCKSDB_NAMESPACE::Status::Code::kIOError:
                return 0x5;
            case ROCKSDB_NAMESPACE::Status::Code::kMergeInProgress:
                return 0x6;
            case ROCKSDB_NAMESPACE::Status::Code::kIncomplete:
                return 0x7;
            case ROCKSDB_NAMESPACE::Status::Code::kShutdownInProgress:
                return 0x8;
            case ROCKSDB_NAMESPACE::Status::Code::kTimedOut:
                return 0x9;
            case ROCKSDB_NAMESPACE::Status::Code::kAborted:
                return 0xA;
            case ROCKSDB_NAMESPACE::Status::Code::kBusy:
                return 0xB;
            case ROCKSDB_NAMESPACE::Status::Code::kExpired:
                return 0xC;
            case ROCKSDB_NAMESPACE::Status::Code::kTryAgain:
                return 0xD;
            case ROCKSDB_NAMESPACE::Status::Code::kColumnFamilyDropped:
                return 0xE;
            default:
                return 0x7F;  // undefined
        }
    }

    // Returns the equivalent org.rocksdb.Status.SubCode for the provided
    // C++ ROCKSDB_NAMESPACE::Status::SubCode enum
    static jbyte toJavaStatusSubCode(const ROCKSDB_NAMESPACE::Status::SubCode& subCode)
    {
        switch (subCode) {
            case ROCKSDB_NAMESPACE::Status::SubCode::kNone:
                return 0x0;
            case ROCKSDB_NAMESPACE::Status::SubCode::kMutexTimeout:
                return 0x1;
            case ROCKSDB_NAMESPACE::Status::SubCode::kLockTimeout:
                return 0x2;
            case ROCKSDB_NAMESPACE::Status::SubCode::kLockLimit:
                return 0x3;
            case ROCKSDB_NAMESPACE::Status::SubCode::kNoSpace:
                return 0x4;
            case ROCKSDB_NAMESPACE::Status::SubCode::kDeadlock:
                return 0x5;
            case ROCKSDB_NAMESPACE::Status::SubCode::kStaleFile:
                return 0x6;
            case ROCKSDB_NAMESPACE::Status::SubCode::kMemoryLimit:
                return 0x7;
            default:
                return 0x7F;  // undefined
        }
    }

    static std::unique_ptr<ROCKSDB_NAMESPACE::Status> toCppStatus(const jbyte jcode_value, const jbyte jsub_code_value)
    {
        std::unique_ptr<ROCKSDB_NAMESPACE::Status> status;
        switch (jcode_value) {
            case 0x0:
                // Ok
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::OK()));
                break;
            case 0x1:
                // NotFound
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::NotFound(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0x2:
                // Corruption
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::Corruption(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0x3:
                // NotSupported
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::NotSupported(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0x4:
                // InvalidArgument
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::InvalidArgument(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0x5:
                // IOError
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::IOError(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0x6:
                // MergeInProgress
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::MergeInProgress(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0x7:
                // Incomplete
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::Incomplete(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0x8:
                // ShutdownInProgress
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::ShutdownInProgress(
                                    SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0x9:
                // TimedOut
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::TimedOut(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0xA:
                // Aborted
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::Aborted(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0xB:
                // Busy
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::Busy(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0xC:
                // Expired
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::Expired(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0xD:
                // TryAgain
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::TryAgain(SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0xE:
                // ColumnFamilyDropped
                status = std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
                        new ROCKSDB_NAMESPACE::Status(
                                ROCKSDB_NAMESPACE::Status::ColumnFamilyDropped(
                                    SubCodeJni::toCppSubCode(jsub_code_value))));
                break;
            case 0x7F:
            default:
                return nullptr;
        }
        return status;
    }

    // Returns the equivalent ROCKSDB_NAMESPACE::Status for the Java
    // org.rocksdb.Status
    static std::unique_ptr<ROCKSDB_NAMESPACE::Status> toCppStatus(JNIEnv* env, const jobject jstatus)
    {
        jmethodID mid_code = getCodeMethod(env);
        if (mid_code == nullptr) {
            // exception occurred
            return nullptr;
        }
        jobject jcode = env->CallObjectMethod(jstatus, mid_code);
        if (env->ExceptionCheck()) {
            // exception occurred
            return nullptr;
        }

        jmethodID mid_code_value = CodeJni::getValueMethod(env);
        if (mid_code_value == nullptr) {
            // exception occurred
            return nullptr;
        }
        jbyte jcode_value = env->CallByteMethod(jcode, mid_code_value);
        if (env->ExceptionCheck()) {
            // exception occurred
            if (jcode != nullptr) {
                env->DeleteLocalRef(jcode);
            }
            return nullptr;
        }

        jmethodID mid_subCode = getSubCodeMethod(env);
        if (mid_subCode == nullptr) {
            // exception occurred
            return nullptr;
        }
        jobject jsubCode = env->CallObjectMethod(jstatus, mid_subCode);
        if (env->ExceptionCheck()) {
            // exception occurred
            if (jcode != nullptr) {
                env->DeleteLocalRef(jcode);
            }
            return nullptr;
        }

        jbyte jsub_code_value = 0x0;  // None
        if (jsubCode != nullptr) {
            jmethodID mid_subCode_value = SubCodeJni::getValueMethod(env);
            if (mid_subCode_value == nullptr) {
                // exception occurred
                return nullptr;
            }
            jsub_code_value = env->CallByteMethod(jsubCode, mid_subCode_value);
            if (env->ExceptionCheck()) {
                // exception occurred
                if (jcode != nullptr) {
                    env->DeleteLocalRef(jcode);
                }
                return nullptr;
            }
        }

        jmethodID mid_state = getStateMethod(env);
        if (mid_state == nullptr) {
            // exception occurred
            return nullptr;
        }
        jobject jstate = env->CallObjectMethod(jstatus, mid_state);
        if (env->ExceptionCheck()) {
            // exception occurred
            if (jsubCode != nullptr) {
                env->DeleteLocalRef(jsubCode);
            }
            if (jcode != nullptr) {
                env->DeleteLocalRef(jcode);
            }
            return nullptr;
        }

        std::unique_ptr<ROCKSDB_NAMESPACE::Status> status = toCppStatus(jcode_value, jsub_code_value);

        // delete all local refs
        if (jstate != nullptr) {
            env->DeleteLocalRef(jstate);
        }
        if (jsubCode != nullptr) {
            env->DeleteLocalRef(jsubCode);
        }
        if (jcode != nullptr) {
            env->DeleteLocalRef(jcode);
        }

        return status;
    }
};

/**
* FalconExceptionJni is copied from rocksdb project: java/rocksjni/portal.h. Note that we modify the class to support
* throwing FalconException. This portal class is for com.huawei.falcon.state.FalconException.
* */
class FalconExceptionJni : public JavaException<FalconExceptionJni> {
public:
    /**
     * Get the Java Class com.huawei.falcon.state.FalconException
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Class or nullptr if one of the ClassFormatError, ClassCircularityError, NoClassDefFoundError,
     *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
     */
    static jclass getJClass(JNIEnv *env)
    {
        return JavaException::getJClass(env, "com/huawei/falcon/state/cache/FalconException");
    }

    /**
     * Create and throw a Java FalconException with the provided message
     *
     * @param env A pointer to the Java environment
     * @param msg The message for the exception
     *
     * @return true if an exception was thrown, false otherwise
     */
    static bool ThrowNew(JNIEnv *env, const std::string &msg)
    {
        return JavaException::ThrowNew(env, msg);
    }

    /**
     * Create and throw a Java FalconException with the provided message and status. Note that we directly reuse Status
     * for rocksdb, for that falcon cache will not throw any other exception.
     *
     * If s->ok() == true, then this function will not throw any exception.
     *
     * @param env A pointer to the Java environment
     * @param s The status for the exception
     *
     * @return true if an exception was thrown, false otherwise
     */
    static bool ThrowNew(JNIEnv *env, const std::string &msg, std::unique_ptr<ROCKSDB_NAMESPACE::Status> &s)
    {
        return FalconExceptionJni::ThrowNew(env, msg, *(s.get()));
    }

    /**
     * Create and throw a Java RocksDBException with the provided message
     * and status
     *
     * If s.ok() == true, then this function will not throw any exception.
     *
     * @param env A pointer to the Java environment
     * @param msg The message for the exception
     * @param s The status for the exception
     *
     * @return true if an exception was thrown, false otherwise
     */
    static bool ThrowNew(JNIEnv *env, const std::string &msg, const ROCKSDB_NAMESPACE::Status &s)
    {
        assert(!s.ok());
        if (s.ok()) {
            return false;
        }

        // get the FalconException class
        jclass jclazz = getJClass(env);
        if (jclazz == nullptr) {
            // exception occurred accessing class
            std::cerr << "FalconExceptionJni::ThrowNew/class - Error: unexpected exception!" << std::endl;
            return env->ExceptionCheck();
        }

        // get the constructor of FalconException to be thrown
        jmethodID mid = env->GetMethodID(jclazz, "<init>", "(Ljava/lang/String;Lorg/rocksdb/Status;)V");
        if (mid == nullptr) {
            // exception thrown: NoSuchMethodException or OutOfMemoryError
            std::cerr << "FalconExceptionJni::ThrowNew/cstr - Error: unexpected exception!" << std::endl;
            return env->ExceptionCheck();
        }

        jstring jmsg = env->NewStringUTF(msg.c_str());
        if (jmsg == nullptr) {
            // exception thrown: OutOfMemoryError
            std::cerr << "FalconExceptionJni::ThrowNew/msg - Error: unexpected exception!" << std::endl;
            return env->ExceptionCheck();
        }

        // get the Java status object
        jobject jstatus = StatusJni::construct(env, s);
        if (jstatus == nullptr) {
            // exception occcurred
            std::cerr << "FalconExceptionJni::ThrowNew/StatusJni - Error: unexpected exception!" << std::endl;
            if (jmsg != nullptr) {
                env->DeleteLocalRef(jmsg);
            }
            return env->ExceptionCheck();
        }

        // construct the FalconException
        jthrowable falcon_exception = reinterpret_cast<jthrowable>(env->NewObject(jclazz, mid, jmsg, jstatus));
        if (env->ExceptionCheck()) {
            if (jstatus != nullptr) {
                env->DeleteLocalRef(jstatus);
            }
            if (jmsg != nullptr) {
                env->DeleteLocalRef(jmsg);
            }
            if (falcon_exception != nullptr) {
                env->DeleteLocalRef(falcon_exception);
            }
            std::cerr << "FalconExceptionJni::ThrowNew/NewObject - Error: unexpected exception!" << std::endl;
            return true;
        }

        // throw the FalconException
        const jint rs = env->Throw(falcon_exception);
        if (rs != JNI_OK) {
            // exception could not be thrown
            std::cerr << "FalconExceptionJni::ThrowNew - Fatal: could not throw exception!" << std::endl;
            if (jstatus != nullptr) {
                env->DeleteLocalRef(jstatus);
            }
            if (jmsg != nullptr) {
                env->DeleteLocalRef(jmsg);
            }
            if (falcon_exception != nullptr) {
                env->DeleteLocalRef(falcon_exception);
            }
            return env->ExceptionCheck();
        }

        if (jstatus != nullptr) {
            env->DeleteLocalRef(jstatus);
        }
        if (jmsg != nullptr) {
            env->DeleteLocalRef(jmsg);
        }
        if (falcon_exception != nullptr) {
            env->DeleteLocalRef(falcon_exception);
        }

        return true;
    }

    /**
     * Get the Java Method: RocksDBException#getStatus
     *
     * @param env A pointer to the Java environment
     *
     * @return The Java Method ID or nullptr if the class or method id could not be retrieved
     */
    static jmethodID getStatusMethod(JNIEnv *env)
    {
        jclass jclazz = getJClass(env);
        if (jclazz == nullptr) {
            // exception occurred accessing class
            return nullptr;
        }

        static jmethodID mid = env->GetMethodID(jclazz, "getStatus", "()Lorg/rocksdb/Status;");
        assert(mid != nullptr);
        return mid;
    }

    static std::unique_ptr<ROCKSDB_NAMESPACE::Status> toCppStatus(JNIEnv *env, jthrowable jrocksdb_exception)
    {
        if (!env->IsInstanceOf(jrocksdb_exception, getJClass(env))) {
            // not an instance of RocksDBException
            return nullptr;
        }

        // get the java status object
        jmethodID mid = getStatusMethod(env);
        if (mid == nullptr) {
            // exception occurred accessing class or method
            return nullptr;
        }

        jobject jstatus = env->CallObjectMethod(jrocksdb_exception, mid);
        if (env->ExceptionCheck()) {
            // exception occurred
            return nullptr;
        }

        if (jstatus == nullptr) {
            return nullptr;   // no status available
        }

        return StatusJni::toCppStatus(env, jstatus);
    }
};

}
#endif // FALCON_FALCONEXCEPTIONJNI_H
