# This project has 2 hash maps

## DiskHashMap is backed by mmapped files

1. It is a single-threaded map with an open addressing scheme
2. It has a store of entries in a file and stores for keys and values in separate files
3. Every time it needs to grow it doubles the size of the file

## ConcurrentDiskHashMap is backed by mmapped files and uses a sharded locking mechanism similar to DashMap
