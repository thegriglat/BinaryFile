#ifndef _BINARY_FILE_H
#define _BINARY_FILE_H

#include <fstream>
#include <vector>
#include <algorithm>
#include <cstring>
#include <iostream>
using namespace std;

#define ZLIB_VERSION
#ifdef ZLIB_VERSION
#define USE_ZLIB 1

#ifndef CHUNK
#define CHUNK 16384
#else
#error CHUNK
#endif

// libz helpers
#include <cstdlib>
#include "zlib.h"

struct BunchHeader
{
    unsigned int compressedSize;
    unsigned int chunkCount;
};

struct Compressed
{
    Bytef *data;
    uLongf CompressedSize;
    uLong UncompressedSize;
};

struct Uncompressed
{
    Bytef *data;
    uLong size;
};

Compressed gz(Bytef *data, uLong size, int compressionLevel)
{
    uLongf destLen = compressBound(size);
    Bytef *tgt = new Bytef[destLen];
    compress2(tgt, &destLen, data, size, compressionLevel);
    return {
        .data = tgt,
        .CompressedSize = destLen,
        .UncompressedSize = size};
}
Uncompressed ungz(Compressed data)
{
    Bytef *tgt = new Bytef[data.UncompressedSize];
    uLongf usize = data.UncompressedSize;
    uncompress(tgt, &usize, data.data, data.CompressedSize);
    return {
        .data = tgt,
        .size = usize};
}
#endif

// like Option type
template <typename T>
struct Result
{
    bool state;
    T result;
    operator bool() const
    {
        return state;
    }

    operator T() const
    {
        return result;
    }

    T operator()() const
    {
        return result;
    }
};

// main template class
template <typename H, typename T>
class BinaryFile
{
private:
    std::fstream _file;
    bool (*_indexFn)(const T &a, const T &b) = nullptr;
    bool _isIndexed = false;
    int binary_search(const T element, int low, int high);
    int _compressionLevel = 6;
    size_t _bunchSize = 1024 / sizeof(T); // N chunks in 100K
    std::vector<unsigned int> _bunchPositions;

public:
    BinaryFile(const char *filename, int compressionLevel = 6, int bunchSize = 1024 / sizeof(T));
    ~BinaryFile() { _file.close(); };
    void close() { _file.close(); }
    int count();

    void start()
    {
        _file.seekg(0);
        _file.seekg(0);
    }
    int getBunchIndex(int chunchPos)
    {
        return chunchPos / _bunchSize;
    }

    void writeHeader(const H &header);
    void readHeader(H &header);

    // write to the end; pos == -1 means end
    void writeChunk(const T &chunk);
    // read chunk at pos
    void readChunk(T &chunk);

    // finds all matching chunks
    std::vector<T> filter(bool (*filterFn)(const T chunk));

    inline std::vector<T> readChunks()
    {
        return filter([](T) { return true; });
    }

    // find first matching chunk
    Result<T> find(bool (*filterFn)(const T &chunk));
    // use qsort
    Result<T> find(const T chunk);
    // sort chunks using _indexFn
    void indexChunks(bool (*fn)(const T &a, const T &b));

    void setIndexFunction(bool (*fn)(const T &a, const T &b))
    {
        _indexFn = fn;
    }

    bool isIndexable() const
    {
        return _indexFn != nullptr;
    }

    bool isIndexed() const
    {
        return _isIndexed;
    }
};

template <typename H, typename T>
BinaryFile<H, T>::BinaryFile(const char *filename, int compressionLevel, int bunchSize)
{
    _bunchSize = bunchSize;
    _compressionLevel = compressionLevel;
    _file.open(filename, std::ios::in | std::ios::out | std::ios::binary);
    if (!_file.is_open())
    {
        // new file;
        _file.open(filename, std::ios::out | std::ios::in | std::fstream::trunc | std::ios::binary);
        if (!_file.is_open())
        {
            exit(1);
        }
        // write header and first bunch
        _file.seekp(0);
        H h;
        _file.write((char *)&h, sizeof(H));
        BunchHeader bh;
        bh.chunkCount = 0;
        bh.compressedSize = 0;
        _file.write((char *)&bh, sizeof(BunchHeader));
        _bunchPositions.push_back(sizeof(H));
        return;
    }
    // not new file;
    // populate bunch positions
    _file.seekg(sizeof(H));
    while (!_file.fail())
    {
        _bunchPositions.push_back(_file.tellg());
        BunchHeader bh;
        _file.read((char *)&bh, sizeof(BunchHeader));
        _file.seekg(bh.compressedSize, _file.cur);
    };
    _file.clear();
}

template <typename H, typename T>
int BinaryFile<H, T>::count()
{
    const auto pos = _file.tellg();
    int count = 0;
    for (auto bpos : _bunchPositions)
    {
        _file.seekg(bpos);
        BunchHeader bh;
        _file.read((char *)&bh, sizeof(BunchHeader));
        count += bh.chunkCount;
    }
    _file.seekg(pos);
    return count;
}

template <typename H, typename T>
void BinaryFile<H, T>::writeHeader(const H &header)
{
    const auto pos = _file.tellp();
    _file.seekp(0);
    _file.write((char *)&header, sizeof(H));
    _file.seekp(pos);
}

template <typename H, typename T>
void BinaryFile<H, T>::readHeader(H &header)
{
    const auto pos = _file.tellg();
    _file.seekg(0, _file.beg);
    _file.read((char *)&header, sizeof(H));
    _file.seekg(pos);
}

// write to the end; pos == -1 means end
template <typename H, typename T>
void BinaryFile<H, T>::writeChunk(const T &chunk)
{
    BunchHeader lastBunch;
    _file.seekg(_bunchPositions[_bunchPositions.size() - 1]);
    _file.read((char *)&lastBunch, sizeof(BunchHeader));
    if (lastBunch.chunkCount + 1 < _bunchSize)
    {
        // write in current bunch
        // read all bunch
        unsigned char *updatedChunks = new unsigned char[(lastBunch.chunkCount + 1) * sizeof(T)];
        if (lastBunch.chunkCount != 0)
        {
            char *bunchData = new char[lastBunch.compressedSize];
            _file.read(bunchData, lastBunch.compressedSize);
            Compressed c;
            c.data = (Bytef *)bunchData;
            c.CompressedSize = lastBunch.compressedSize;
            c.UncompressedSize = lastBunch.chunkCount * sizeof(T);
            Uncompressed r = ungz(c);
            std::memcpy(updatedChunks, r.data, r.size);
            delete[] bunchData;
        }
        unsigned char *updatePositionChunk = updatedChunks + lastBunch.chunkCount * sizeof(T);
        // append chunk to uncompressed data
        std::memcpy(updatePositionChunk, &chunk, sizeof(T));

        Compressed out = gz(updatedChunks, (lastBunch.chunkCount + 1) * sizeof(T), _compressionLevel);
        // update bunch header and data
        _file.seekp(_bunchPositions[_bunchPositions.size() - 1]);
        lastBunch.chunkCount += 1;
        lastBunch.compressedSize = out.CompressedSize;
        _file.write((char *)&lastBunch, sizeof(BunchHeader));
        _file.write((char *)updatedChunks, out.CompressedSize);
        delete[] updatedChunks;
    }
    else
    {
        // write new bunch
        _file.seekp(0, _file.end);
        BunchHeader bh;
        bh.chunkCount = 0;
        bh.compressedSize = 0;
        _bunchPositions.push_back(_file.tellp());
        _file.write((char *)&bh, sizeof(BunchHeader));
        writeChunk(chunk);
    }
    _file.seekp(0, _file.end);
    _isIndexed = false;
}

// read chunk at pos
template <typename H, typename T>
void BinaryFile<H, T>::readChunk(T &chunk)
{
    if (_file.tellg() < (int)sizeof(H))
        _file.seekg(sizeof(H));
    Result<T> result;
    result.state = false;
    _file.read((char *)&(result.result), sizeof(T));
    if (!_file.fail())
    {
        result.state = true;
    }
    else
    {
        // clear failbit
        _file.clear();
    }
    return result;
}

template <typename H, typename T>
void BinaryFile<H, T>::indexChunks(bool (*fn)(const T &a, const T &b))
{
    setSortFunction(fn);
    std::vector<T> chunks = readChunks();
    std::sort(chunks.begin(), chunks.end(), _indexFn);
    _file.seekp(sizeof(H));
    for (auto &item : chunks)
    {
        writeChunk(item);
    }
    _isIndexed = true;
}

template <typename H, typename T>
int BinaryFile<H, T>::binary_search(const T chunk, int low, int high)
{
    if (!isIndexable())
        return -1;
    int right = high;
    int left = low;
    int mid = low;
    auto equal = [this](T a, T b) {
        return !_indexFn(a, b) && !_indexFn(b, a);
    };
    while (right - left >= 1)
    {
        mid = (right + left) / 2;
        const T midc = readChunk(mid);
        bool sort_result = _indexFn(chunk, midc);
        if (equal(chunk, midc))
        {
            return mid;
        }
        if (sort_result)
        {
            right = mid;
        }
        else
        {
            left = mid;
        }
        if (right - left == 1 && mid == left)
            break;
    }
    // not found
    return -1;
}

template <typename H, typename T>
Result<T> BinaryFile<H, T>::find(const T chunk)
{
    if (!isIndexable())
    {
        // TODO: operator== for T must be defined
        /*
        return find([chunk](T ch) {
            return ch == chunk;
        });
        */
        Result<T> r;
        r.state = false;
        return r;
    }
    auto pos = binary_search(chunk, 0, count());
    if (pos >= 0)
    {
        return readChunk(pos);
    }
    Result<T> r;
    r.state = false;
    return r;
}

template <typename H, typename T>
std::vector<T> BinaryFile<H, T>::filter(bool (*filterFn)(const T chunk))
{
    const auto pos = _file.tellg();
    std::vector<T> res;
    res.reserve(count());
    Result<T> iter;
    _file.seekg(sizeof(H));
    while ((iter = readChunk()))
    {
        if (filterFn(iter))
            res.push_back(iter);
    }
    // reset pos
    _file.seekg(pos);
    return res;
}

template <typename H, typename T>
Result<T> BinaryFile<H, T>::find(bool (*filterFn)(const T &chunk))
{
    const auto pos = _file.tellg();
    Result<T> iter;
    _file.seekg(sizeof(H));
    while ((iter = readChunk()))
    {
        if (filterFn(iter))
        {
            _file.seekg(pos);
            return iter;
        }
    }
    Result<T> r;
    r.state = false;
    _file.seekg(pos);
    return r;
};

#undef USEZLIB
#endif