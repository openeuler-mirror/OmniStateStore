set(SECURE_SRC_PATH ${PROJECT_3RDPARTY_SRC_DIR}/secure/libboundscheck)
set(BUILD_DIR       ${PROJECT_BUILD_PATH}/secure_build)
set(SECURE_INSTALL_DIR ${PROJECT_3RDPARTY_BIN_DIR}/libboundscheck)
 
message(${SECURE_INSTALL_DIR}/lib/)
set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 11)
 
find_file(SECURE_LIB_PATH libboundscheck.so ${SECURE_INSTALL_DIR}/lib/)
if(NOT EXISTS ${SECURE_LIB_PATH})
    EXEC_PROGRAM(cp ${BUILD_DIR} ARGS -rf ${SECURE_SRC_PATH} ${BUILD_DIR})
    EXEC_PROGRAM(make ${BUILD_DIR}/libboundscheck ARGS -j4)
 
    EXEC_PROGRAM(cp ${SECURE_INSTALL_DIR} ARGS -rf ${BUILD_DIR}/libboundscheck/lib/ ${SECURE_INSTALL_DIR})
    EXEC_PROGRAM(cp ${SECURE_INSTALL_DIR} ARGS -rf ${SECURE_SRC_PATH}/include ${SECURE_INSTALL_DIR})
    #create .a
    EXEC_PROGRAM(ar ${SECURE_INSTALL_DIR}/lib ARGS crv ${SECURE_INSTALL_DIR}/lib/libboundscheck.a ${BUILD_DIR}/libboundscheck/obj/*.o)
 
else(NOT EXISTS ${SECURE_LIB_PATH})
    message("-- libboundscheck: has been built, ignored")
endif(NOT EXISTS ${SECURE_LIB_PATH})
