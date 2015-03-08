solution "MooDB"
  configurations { "Debug", "Release" }

project "MooDBCore"
  kind "SharedLib"
  language "C++"
  location "build/core"
  files { "src/core/*.cpp", "src/core/*.h" }
  includedirs { "include" }
  links { "db" }
