#ifndef _BINARY_FILE_H
#define _BINARY_FILE_H

#include <fstream>
#include <vector>
#include <algorithm>
#include <cstring>
#include "zlib.h"

// main template class
template <typename H, typename T>
class BinaryFile
{

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

    typedef std::vector<Bytef> Uncompressed;

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
        Uncompressed tgt(data.UncompressedSize);
        uLongf usize = data.UncompressedSize;
        uncompress(&(tgt[0]), &usize, data.data, data.CompressedSize);
        tgt.resize(usize);
        return tgt;
    }

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
    Uncompressed currentBunchData = {};
    Uncompressed currentWriteBunchData = {};

    void setReadPos(size_t newpos)
    {
        currentChunk = newpos;
    }

    size_t getBunchIndex(int chunchPos)
    {
        return chunchPos / _bunchSize;
    }

    bool _isSynced = true;
    void sync()
    {
        if (_isSynced)
            return;
        BunchHeader lastBunch;
        _file.seekg(_bunchPositions[_bunchPositions.size() - 1]);
        _file.read((char *)&lastBunch, sizeof(BunchHeader));
        Compressed out = gz(&(currentWriteBunchData[0]), currentWriteBunchData.size(), _compressionLevel);
        lastBunch.compressedSize = out.CompressedSize;
        _file.seekp(_bunchPositions[_bunchPositions.size() - 1]);
        _file.write((char *)&lastBunch, sizeof(BunchHeader));
        _file.write((char *)out.data, out.CompressedSize);
        delete[] out.data;
        currentWriteBunchData.clear();
        _isSynced = true;
        // synchronize unwritten buffer with disk
    }

public:
    BinaryFile(const char *filename, int compressionLevel = 6, int bunchSize = 1024 / sizeof(T));

    void close()
    {
        sync();
        _file.close();
    }
    ~BinaryFile()
    {
        close();
    };
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
    _file.seekg(0, _file.end);
    size_t _end = _file.tellg();
    size_t pos = sizeof(H);
    do
    {
        _file.seekg(pos);
        _bunchPositions.push_back(pos);
        BunchHeader bh;
        _file.read((char *)&bh, sizeof(BunchHeader));
        _file.seekg(bh.compressedSize, _file.cur);
        pos += sizeof(BunchHeader) + bh.compressedSize;

    } while (pos < _end);
    _file.clear();
}

template <typename H, typename T>
int BinaryFile<H, T>::count()
{
    int count = 0;
    for (auto bpos : _bunchPositions)
    {
        _file.seekg(bpos);
        BunchHeader bh;
        _file.read((char *)&bh, sizeof(BunchHeader));
        count += bh.chunkCount;
    }
    return count;
}

template <typename H, typename T>
void BinaryFile<H, T>::writeHeader(const H &header)
{
    _file.seekp(0);
    _file.write((char *)&header, sizeof(H));
}

template <typename H, typename T>
void BinaryFile<H, T>::readHeader(H &header)
{
    _file.seekg(0, _file.beg);
    _file.read((char *)&header, sizeof(H));
}

// write to the end; pos == -1 means end
template <typename H, typename T>
void BinaryFile<H, T>::writeChunk(const T &chunk)
{
    BunchHeader lastBunch;
    _file.seekg(_bunchPositions[_bunchPositions.size() - 1]);
    _file.read((char *)&lastBunch, sizeof(BunchHeader));
    if (lastBunch.chunkCount + 1 > _bunchSize)
    {
        // write new bunch
        _file.seekp(lastBunch.compressedSize, _file.cur);
        lastBunch.chunkCount = 0;
        lastBunch.compressedSize = 0;
        _bunchPositions.push_back(_file.tellp());
        _file.write((char *)&lastBunch, sizeof(BunchHeader));
        // reset
        currentWriteBunchData.clear();
    };

    // read all bunch data
    if (currentWriteBunchData.size() != 0)
    {
        // realloc current buffer
        currentWriteBunchData.resize(currentWriteBunchData.size() + sizeof(T));
    }
    else
    {
        currentWriteBunchData.resize(sizeof(T));
    }
    Bytef *updatedChunks = &(currentWriteBunchData[0]);
    Bytef *updatedChunks_start = updatedChunks;
    Bytef *updatePositionChunk = updatedChunks_start + lastBunch.chunkCount * sizeof(T);
    // append chunk to uncompressed data
    std::memcpy(updatePositionChunk, &chunk, sizeof(T));
    lastBunch.chunkCount += 1;
    lastBunch.compressedSize = 0;
    _file.seekp(_bunchPositions[_bunchPositions.size() - 1]);
    _file.write((char *)&lastBunch, sizeof(BunchHeader));
    // update bunch header
    if (lastBunch.chunkCount == _bunchSize)
    {
        // write only at the end of bunch
        // or at sync()
        sync();
    }
    _isSynced = false;
    _file.seekp(0, _file.end);
    _isIndexed = false;
}

// read chunk at pos
template <typename H, typename T>
void BinaryFile<H, T>::readChunk(T &chunk)
{
    sync();
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
        currentBunchData = ungz(c);
        currentBunch = bunchIdx;
        delete[] bunchData;
    }
    auto chunkPosInBunch = currentChunk % _bunchSize;
    Uncompressed tmp(currentBunchData.begin() + chunkPosInBunch * sizeof(T), currentBunchData.begin() + (chunkPosInBunch + 1) * sizeof(T));
    chunk = *((T *)&(tmp[0]));
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