#! /bin/sh

# sbt clean leaves a bunch of stuff behind.
rm -rf lib_managed
rm -rf target
rm -rf project/boot
rm -rf project/build/target
