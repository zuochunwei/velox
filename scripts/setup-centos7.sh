#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -efx -o pipefail
# Some of the packages must be build with the same compiler flags
# so that some low level types are the same size. Also, disable warnings.
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh
CPU_TARGET="${CPU_TARGET:-avx}"
NPROC=$(getconf _NPROCESSORS_ONLN)
export CFLAGS=$(get_cxx_flags $CPU_TARGET)  # Used by LZO.
export CXXFLAGS=$CFLAGS  # Used by boost.
export CPPFLAGS=$CFLAGS  # Used by LZO.
FB_OS_VERSION=v2022.11.14.00

function run_and_time {
  time "$@"
  { echo "+ Finished running $*"; } 2> /dev/null
}

function dnf_install {
  dnf install -y -q --setopt=install_weak_deps=False "$@"
}

function yum_install {
  yum install -y "$@"
}

function cmake_install_deps {
  cmake -B"$1-build" -GNinja -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_CXX_FLAGS="${CFLAGS}" -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release -Wno-dev "$@"
  ninja -C "$1-build" install
}

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DIR}"
  wget -q --max-redirect 3 -O - "${URL}" | tar -xz -C "${DIR}" --strip-components=1
}

function install_cmake {
  wget_and_untar https://cmake.org/files/v3.25/cmake-3.25.1.tar.gz cmake-3
  cd cmake-3
  ./bootstrap --prefix=/usr/local
  make -j${nproc}
  make install
  cmake --version
}

function install_ninja {
  github_checkout ninja-build/ninja v1.11.1
  ./configure.py --bootstrap
  cmake -Bbuild-cmake
  cmake --build build-cmake
  cp ninja /usr/local/bin/  
}

function install_fmt {
  github_checkout fmtlib/fmt 8.0.0
  cmake_install -DFMT_TEST=OFF
}

function install_folly {
  github_checkout facebook/folly "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_conda {
  mkdir -p conda && cd conda
  wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
  MINICONDA_PATH=/opt/miniconda-for-velox
  bash Miniconda3-latest-Linux-x86_64.sh -b -u $MINICONDA_PATH
}

function install_openssl {
  wget_and_untar https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_1_1_1s.tar.gz openssl
  cd openssl
  ./config
  make depend
  make
  make install
  mv /usr/bin/openssl /usr/bin/openssl.bak
  ln -s /usr/local/ssl/bin/openssl /usr/bin/openssl  
}

function install_gflags {
  wget_and_untar https://github.com/gflags/gflags/archive/v2.2.2.tar.gz gflags
  cd gflags
  cmake_install -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON -DLIB_SUFFIX=64 -DCMAKE_INSTALL_PREFIX:PATH=/usr/local
}

function install_glog {
  wget_and_untar https://github.com/google/glog/archive/v0.5.0.tar.gz glog
  cd glog
  cmake_install -DBUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX:PATH=/usr/local  
}

function install_snappy {
  wget_and_untar https://github.com/google/snappy/archive/1.1.8.tar.gz snappy
  cd snappy
  cmake_install -DSNAPPY_BUILD_TESTS=OFF  
}

function install_dwarf {
  wget_and_untar https://github.com/davea42/libdwarf-code/archive/refs/tags/20210528.tar.gz dwarf
  cd dwarf
  #local URL=https://github.com/davea42/libdwarf-code/releases/download/v0.5.0/libdwarf-0.5.0.tar.xz
  #local DIR=dwarf
  #mkdir -p "${DIR}"
  #wget -q --max-redirect 3 "${URL}"
  #tar -xf libdwarf-0.5.0.tar.xz -C "${DIR}"
  #cd dwarf/libdwarf-0.5.0
  ./configure --enable-shared=yes
  make
  make check
  make install
}

function install_prerequisites {
  run_and_time install_openssl
  run_and_time install_gflags
  run_and_time install_glog
  run_and_time install_snappy
  run_and_time install_dwarf
}

function install_velox_deps {
  run_and_time install_fmt
  run_and_time install_folly
  run_and_time install_conda
}

# CMAKE-3 install
run_and_time install_cmake

# ninja-build install
run_and_time install_ninja

# dnf install dependency libraries
dnf_install epel-release dnf-plugins-core # For ccache, ninja
# PowerTools only works on CentOS8
# dnf config-manager --set-enabled powertools
dnf_install ccache git wget which libevent-devel \
  openssl-devel re2-devel libzstd-devel lz4-devel double-conversion-devel \
  curl-devel cmake

dnf remove -y gflags

# Required for Thrift
dnf_install autoconf automake libtool bison flex python3

dnf_install conda

# Activate gcc9; enable errors on unset variables afterwards.
# GCC9 install via yum and devtoolset
# dnf install gcc-toolset-9 only works on CentOS8
yum_install centos-release-scl
yum_install devtoolset-9
source /opt/rh/devtoolset-9/enable || exit 1
gcc --version
set -u

# Build from Sources
# Fetch sources.
wget_and_untar http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz lzo &
wget_and_untar https://boostorg.jfrog.io/artifactory/main/release/1.72.0/source/boost_1_72_0.tar.gz boost &
#wget_and_untar https://github.com/fmtlib/fmt/archive/8.0.0.tar.gz fmt &

wait  # For cmake and source downloads to complete.

# Build & install.
# install lzo
(
  cd lzo
  ./configure --prefix=/usr/local --enable-shared --disable-static --docdir=/usr/local/share/doc/lzo-2.10
  make "-j$(nproc)"
  make install
)

# install boost
(
  cd boost
  ./bootstrap.sh --prefix=/usr/local --with-python=/usr/bin/python3 --with-python-root=/usr/lib/python3.6
  ./b2 "-j$(nproc)" -d0 install threading=multi
)

# install prerequisites
install_prerequisites

# install fmt
#cmake_install fmt -DFMT_TEST=OFF

install_velox_deps

dnf clean all
