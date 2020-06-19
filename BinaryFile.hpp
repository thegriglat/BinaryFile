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
    size_t _bunchSize = 1024 / sizeof(T); // N chunks in 1K
    std::vector<unsigned int> _bunchPositions;
    size_t currentChunk = 0;
    size_t currentBunch = -1;
    Uncompressed currentBunchData = {.data = nullptr, .size = 0};

    void setReadPos(size_t newpos)
    {
        currentChunk = newpos;
    }

    size_t getBunchIndex(int chunchPos)
    {
        return chunchPos / (_bunchSize - 1);
    }

public:
    BinaryFile(const char *filename, int compressionLevel = 6, int bunchSize = 1024 / sizeof(T));
    ~BinaryFile()
    {
        if (currentBunchData.data)
            delete[] currentBunchData.data;
        _file.close();
    };
    void close() { _file.close(); }
    int count();

    void writeHeader(const H &header);
    void readHeader(H &header);

    // write to the end; pos == -1 means end
    void writeChunk(const T &chunk);
    // read chunk at pos
    void readChunk(T &chunk);
    void readChunk(T &chunk, size_t position);

    // finds all matching chunks
    std::vector<T> filter(bool (*filterFn)(const T chunk));

    inline std::vector<T> readChunks()
    {
        return filter([](T) { return true; });
    }

    // find first matching chunk
    int find(bool (*filterFn)(const T &chunk));
    // use qsort
    int find(const T &chunk);
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
    if (!_file.good())
    {
        // new file;
        _file.open(filename, std::ios::out | std::ios::in | std::fstream::trunc | std::ios::binary);
        if (!_file.good())
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
        Bytef *updatedChunks = new Bytef[(lastBunch.chunkCount + 1) * sizeof(T)];
        Bytef *updatedChunks_start = updatedChunks;
        if (lastBunch.chunkCount != 0)
        {
            Bytef *bunchData = new Bytef[lastBunch.compressedSize];
            auto startpos = _bunchPositions[_bunchPositions.size() - 1] + sizeof(BunchHeader);
            _file.seekg(startpos);
            _file.read((char *)bunchData, lastBunch.compressedSize);
            Compressed c;
            c.data = bunchData;
            c.CompressedSize = lastBunch.compressedSize;
            c.UncompressedSize = lastBunch.chunkCount * sizeof(T);
            Uncompressed r = ungz(c);
            std::memcpy(updatedChunks, r.data, r.size);
            delete[] bunchData;
            delete[] r.data;
        }
        Bytef *updatePositionChunk = updatedChunks_start + lastBunch.chunkCount * sizeof(T);
        // append chunk to uncompressed data
        std::memcpy(updatePositionChunk, &chunk, sizeof(T));

        Compressed out = gz(updatedChunks, (lastBunch.chunkCount + 1) * sizeof(T), _compressionLevel);
        _file.seekp(_bunchPositions[_bunchPositions.size() - 1]);
        lastBunch.chunkCount += 1;
        lastBunch.compressedSize = out.CompressedSize;
        _file.write((char *)&lastBunch, sizeof(BunchHeader));
        _file.write((char *)out.data, out.CompressedSize);
        delete[] out.data;
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
    const auto bunchIdx = getBunchIndex(currentChunk);
    if (bunchIdx != currentBunch)
    {
        // read bunch data
        const auto bunchpos = _bunchPositions.at(bunchIdx);
        BunchHeader bunch;
        _file.seekg(bunchpos);
        _file.read((char *)&bunch, sizeof(BunchHeader));
        // read gzipped data
        char *bunchData = new char[bunch.compressedSize];
        _file.read(bunchData, bunch.compressedSize);
        Compressed c;
        c.data = (Bytef *)bunchData;
        c.CompressedSize = bunch.compressedSize;
        c.UncompressedSize = bunch.chunkCount * sizeof(T);
        delete[] currentBunchData.data;
        currentBunchData = ungz(c);
        currentBunch = bunchIdx;
        delete[] bunchData;
    }
    auto chunkPosInBunch = currentChunk % (_bunchSize - 1);
    Bytef *tmp = new Bytef[sizeof(T)];
    std::memcpy(tmp, currentBunchData.data + chunkPosInBunch * sizeof(T), sizeof(T));
    chunk = *((T *)tmp);
    delete[] tmp;
    currentChunk++;
}

template <typename H, typename T>
void BinaryFile<H, T>::readChunk(T &chunk, size_t position)
{
    setReadPos(position);
    readChunk(chunk);
}

template <typename H, typename T>
void BinaryFile<H, T>::indexChunks(bool (*fn)(const T &a, const T &b))
{
    setIndexFunction(fn);
    std::vector<T> chunks = readChunks();
    std::sort(chunks.begin(), chunks.end(), _indexFn);
    _file.seekp(sizeof(H));
    BunchHeader bh;
    bh.chunkCount = 0;
    bh.compressedSize = 0;
    _file.write((char *)&bh, sizeof(BunchHeader));
    currentChunk = 0;
    currentBunch = -1;
    _bunchPositions.clear();
    _bunchPositions.push_back(sizeof(H));
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
        T midc;
        readChunk(midc, mid);
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
int BinaryFile<H, T>::find(const T &chunk)
{
    if (!isIndexable())
    {
        // TODO: operator== for T must be defined
        // TODO: Use find (<lambda>)
        for (int i = 0; i < count(); ++i)
        {
            T item;
            readChunk(item, i);
            if (item == chunk)
                return i;
        }
        return -1;
    }
    auto pos = binary_search(chunk, 0, count());
    if (pos >= 0)
    {
        return pos;
    }
    return -1;
}

template <typename H, typename T>
std::vector<T> BinaryFile<H, T>::filter(bool (*filterFn)(const T chunk))
{
    const auto pos = _file.tellg();
    std::vector<T> res;
    res.reserve(count());
    for (int i = 0; i < count(); ++i)
    {
        T item;
        readChunk(item, i);
        if (filterFn(item))
            res.push_back(item);
    }
    // reset pos
    _file.seekg(pos);
    return res;
}

template <typename H, typename T>
int BinaryFile<H, T>::find(bool (*filterFn)(const T &chunk))
{
    for (int i = 0; i < count(); ++i)
    {
        T item;
        readChunk(item, i);
        if (filterFn(item))
        {
            return i;
        }
    }
    return -1;
};

#undef USEZLIB
#endif