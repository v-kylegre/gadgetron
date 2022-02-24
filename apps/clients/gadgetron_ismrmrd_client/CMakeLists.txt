find_package(ZFP)


if (ZFP_FOUND)
   add_definitions(-DGADGETRON_COMPRESSION_ZFP)
endif()

if (ZFP_FOUND)
   include_directories(${ZFP_INCLUDE_DIR})
endif()

file(GLOB gadgetron_ismrmrd_client_srcs *.cpp *.h)

add_executable(gadgetron_ismrmrd_client ${gadgetron_ismrmrd_client_srcs})

include_directories(${HDF5_INCLUDE_DIRS})
target_link_libraries(gadgetron_ismrmrd_client ISMRMRD::ISMRMRD Boost::program_options gadgetron_mricore )

if (ZFP_FOUND)
   target_link_libraries(gadgetron_ismrmrd_client ${ZFP_LIBRARIES})
endif ()

install(TARGETS gadgetron_ismrmrd_client DESTINATION bin COMPONENT main)
