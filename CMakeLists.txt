

if($ENV{Athena_SETUP} STREQUAL "on")
  message(STATUS "skip stansalone cmake list")
else()

  cmake_minimum_required(VERSION 3.0)
  
  project(saphyra)
  
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -DDROP_CGAL")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
  
  # mare ROOT dependenc
  find_package(ROOT COMPONENTS EG Eve Geom Gui GuiHtml GenVector Hist Physics Matrix Graf RIO Tree Gpad RGL MathCore)
  include(${ROOT_USE_FILE})
  include(Gaugi/cmake/InstallGaugiFunctions.cmake)
  
  gaugi_initialize()
  
  set( Python_ADDITIONAL_VERSIONS 2.7.13 2.7.5)
  find_package( PythonInterp REQUIRED)
  find_package( PythonLibs REQUIRED)
  find_package( Boost COMPONENTS python system filesystem)
  include_directories( ${Boost_INCLUDE_DIR} )
  link_directories(${Boost_LIBRARY_DIR})
 
  ### If needed to find numpy check caffe script
  # https://github.com/PMBio/peer/blob/master/cmake/FindNumpy.cmake 
  if (NOT NUMPY_INCLUDE_DIR)
      exec_program ("${PYTHON_EXECUTABLE}"
        ARGS "-c 'import numpy; print (numpy.get_include())'"
        OUTPUT_VARIABLE NUMPY_INCLUDE_DIR
        RETURN_VALUE NUMPY_NOT_FOUND)
      if (NUMPY_INCLUDE_DIR MATCHES "Traceback")
      # Did not successfully include numpy
        set(NUMPY_FOUND FALSE)
      else (NUMPY_INCLUDE_DIR MATCHES "Traceback")
      # successful
        set (NUMPY_FOUND TRUE)
        set (NUMPY_INCLUDE_DIR ${NUMPY_INCLUDE_DIR} CACHE PATH "Numpy include path")
      endif (NUMPY_INCLUDE_DIR MATCHES "Traceback")
      if (NUMPY_FOUND)
        if (NOT NUMPY_FIND_QUIETLY)
          message (STATUS "Numpy headers found")
        endif (NOT NUMPY_FIND_QUIETLY)
      else (NUMPY_FOUND)
        if (NUMPY_FIND_REQUIRED)
          message (FATAL_ERROR "Numpy headers missing")
        endif (NUMPY_FIND_REQUIRED)
      endif (NUMPY_FOUND)
      mark_as_advanced (NUMPY_INCLUDE_DIR)
  endif (NOT NUMPY_INCLUDE_DIR) 
  
  
  include_directories(${NUMPY_INCLUDE_DIR})
  
  
  if(NUMPY_FOUND)
    message(STATUS "NumPy ver. ${NUMPY_VERSION} found (include: ${NUMPY_INCLUDE_DIR})")
  endif()
  
  
  # Component(s) in the package:
  find_package(OpenMP)
  if (OPENMP_FOUND)
      set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
      set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
      set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
  endif()


  
  
  
  add_subdirectory(Gaugi)
  add_subdirectory(External/ringerdb)
  add_subdirectory(orchestra)

  add_library(saphyra SHARED
    $<TARGET_OBJECTS:Gaugi> 
    #$<TARGET_OBJECTS:TuningTools> 
  )
  
  
  target_link_Libraries(saphyra ${ROOT_LIBRARIES} ${ROOT_COMPONENT_LIBRARIES} ${Boost_PYTHON_LIBRARIES} ${PYTHON_LIBRARIES} ${Boost_LIBRARIES})

endif()

