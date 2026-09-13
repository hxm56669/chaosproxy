function(cp_project_target target)
  target_compile_features(${target} PUBLIC cxx_std_20)
  target_compile_options(${target} PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
  if(CP_WARNINGS_AS_ERRORS)
    target_compile_options(${target} PRIVATE -Werror)
  endif()
  if(CP_SANITIZER STREQUAL "address-undefined")
    target_compile_options(${target} PRIVATE
      -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(${target} PRIVATE -fsanitize=address,undefined)
  elseif(CP_SANITIZER STREQUAL "thread")
    target_compile_options(${target} PRIVATE
      -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(${target} PRIVATE -fsanitize=thread)
  elseif(NOT CP_SANITIZER STREQUAL "none")
    message(FATAL_ERROR "Unknown CP_SANITIZER=${CP_SANITIZER}")
  endif()
endfunction()
