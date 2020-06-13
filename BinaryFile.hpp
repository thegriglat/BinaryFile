#ifndef _BINARY_FILE_H
#define _BINARY_FILE_H

#include <fstream>
#include <vector>
#include <algorithm>

#define ZLIB_VERSION
#ifdef ZLIB_VERSION
#define USE_ZLIB 1

// libz helpers
#include <cstdlib>
#include "zlib.h"

struct Compressed
{
    Bytef *data;
    uLongf CompressedSize;
    uLong UncompressedSize;
    bool compressed;
    ~Compressed()
    {
        if (data)
            delete[] data;
    }
};

struct Uncompressed
{
    Bytef *data;
    uLong size;
    ~Uncompressed()
    {
        if (data)
            delete[] data;
    }
};

Compressed gz(Bytef *data, uLong size)
{
    uLongf destLen = compressBound(size);
    Bytef *tgt = new Bytef[destLen];
    compress(tgt, &destLen, data, size);
    if (destLen < size)
    {
        return {
            .data = tgt,
            .CompressedSize = destLen,
            .UncompressedSize = size,
            .compressed = true};
    }
    // compressed size is greater than original data
    // dont return compressed data
    delete[] tgt;
    return {
        .data = data,
        .CompressedSize = size,
        .UncompressedSize = size,
        .compressed = false};
}
Uncompressed ungz(Compressed data)
{
    if (!data.compressed)
    {
        return {
            .data = data.data,
            .size = data.UncompressedSize};
    }
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
    bool (*_indexFn)(const T a, const T b) = nullptr;
    bool _isIndexed = false;
    int binary_search(const T element, int low, int high);

public:
    BinaryFile(const char *filename);
    ~BinaryFile() { _file.close(); };
    void close() { _file.close(); }
    int count();

    void writeHeader(const H header);
    H readHeader();

    // write to the end; pos == -1 means end
    void writeChunk(const T chunk);
    void writeChunk(const T chunk, int pos);
    // read chunk at pos
    Result<T> readChunk();
    Result<T> readChunk(int pos);

    // finds all matching chunks
    std::vector<T> filter(bool (*filterFn)(const T chunk));

    inline std::vector<T> readChunks()
    {
        return filter([](T) { return true; });
    }

    // find first matching chunk
    Result<T> find(bool (*filterFn)(const T chunk));
    // use qsort
    Result<T> find(const T chunk);
    // sort chunks using _indexFn
    void indexChunks(bool (*fn)(const T a, const T b));

    void setIndexFunction(bool (*fn)(const T a, const T b))
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
BinaryFile<H, T>::BinaryFile(const char *filename)
{
    _file.open(filename, std::ios::in | std::ios::out | std::ios::binary);
    if (!_file.is_open())
    {
        _file.open(filename, std::ios::out | std::ios::in | std::fstream::trunc | std::ios::binary);
        if (!_file.is_open())
        {
            exit(1);
        }
    }
}

template <typename H, typename T>
int BinaryFile<H, T>::count()
{
    const auto pos = _file.tellg();
    _file.seekg(0, _file.end);
    const auto res = ((int)(_file.tellg()) - sizeof(H)) / sizeof(T);
    _file.seekg(pos);
    return res;
}

template <typename H, typename T>
void BinaryFile<H, T>::writeHeader(const H header)
{
    _file.seekp(0);
    _file.write((char *)&header, sizeof(H));
}

template <typename H, typename T>
H BinaryFile<H, T>::readHeader()
{
    const auto pos = _file.tellg();
    _file.seekg(0, _file.beg);
    H header;
    _file.read((char *)&header, sizeof(H));
    _file.seekg(pos);
    return header;
}

// write to the end; pos == -1 means end
template <typename H, typename T>
void BinaryFile<H, T>::writeChunk(const T chunk)
{
    _file.write((char *)&chunk, sizeof(T));
    _isIndexed = false;
}

template <typename H, typename T>
void BinaryFile<H, T>::writeChunk(const T chunk, int pos)
{
    // starts of the data
    if (pos >= 0)
    {
        _file.seekp(sizeof(H) + sizeof(T) * pos);
    }
    else
    {
        // write to the end
        const auto gpos = _file.tellg();
        _file.seekg(0, _file.end);
        const long size = _file.tellg();
        _file.seekg(gpos);
        _file.seekp(size);
    }
    writeChunk(chunk);
};

// read chunk at pos
template <typename H, typename T>
Result<T> BinaryFile<H, T>::readChunk()
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
Result<T> BinaryFile<H, T>::readChunk(int pos)
{
    int position = sizeof(H);
    if (pos >= 0)
    {
        position = sizeof(H) + sizeof(T) * pos;
    }
    _file.seekg(position);
    return readChunk();
}

template <typename H, typename T>
void BinaryFile<H, T>::indexChunks(bool (*fn)(const T a, const T b))
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
Result<T> BinaryFile<H, T>::find(bool (*filterFn)(const T chunk))
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