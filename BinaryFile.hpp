#ifndef _BINARY_FILE_H
#define _BINARY_FILE_H

#include <fstream>
#include <vector>
#include <algorithm>

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

template <typename H, typename T>
class File
{
private:
    std::fstream _file;
    bool (*_indexFn)(const T a, const T b) = nullptr;
    bool _isIndexed = false;
    int binary_search(const T element, int low, int high);

public:
    File(const char *filename);
    ~File() { _file.close(); };
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
File<H, T>::File(const char *filename)
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
int File<H, T>::count()
{
    const auto pos = _file.tellg();
    _file.seekg(0, _file.end);
    const auto res = ((int)(_file.tellg()) - sizeof(H)) / sizeof(T);
    _file.seekg(pos);
    return res;
}

template <typename H, typename T>
void File<H, T>::writeHeader(const H header)
{
    _file.seekp(0);
    _file.write((char *)&header, sizeof(Header));
}

template <typename H, typename T>
H File<H, T>::readHeader()
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
void File<H, T>::writeChunk(const T chunk)
{
    _file.write((char *)&chunk, sizeof(T));
    _isIndexed = false;
}

template <typename H, typename T>
void File<H, T>::writeChunk(const T chunk, int pos)
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
Result<T> File<H, T>::readChunk()
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
Result<T> File<H, T>::readChunk(int pos)
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
void File<H, T>::indexChunks(bool (*fn)(const T a, const T b))
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
int File<H, T>::binary_search(const T chunk, int low, int high)
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
Result<T> File<H, T>::find(const T chunk)
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
std::vector<T> File<H, T>::filter(bool (*filterFn)(const T chunk))
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
Result<T> File<H, T>::find(bool (*filterFn)(const T chunk))
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
}

#endif