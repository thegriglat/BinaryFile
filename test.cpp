#include "BinaryFile.hpp"

#include <iostream>
#include <string>
#include <fstream>

using namespace std;

struct Header
{
    int version = 2;
};
struct Item
{
    int a = -1;
    friend ostream &operator<<(ostream &os, Item &q)
    {
        os << q.a;
        return os;
    };
    friend bool operator==(const Item &lhs, const Item &rhs)
    {
        return lhs.a == rhs.a; /* your comparison code goes here */
    }
};

int main(int argc, char **argv)
{
    int c = 5;
    if (argc > 1)
    {
        c = atoi(argv[1]);
    }
    BinaryFile<Header, Item> bf("binary.bin", 9);
    bf.writeHeader({.version = 2});
    cout << "### WRITE ###" << endl;
    for (int i = c - 1; i >= 0; --i)
    {
        bf.writeChunk({.a = i * i});
    }
    cout << "### WRITE END ###" << endl;
    cout << "count = " << bf.count() << endl;
    // bf.setIndexFunction([](const Item &a, const Item &b) { return a.a < b.b; });
    // bf.indexChunks([](const Item &a, const Item &b) { return a.a < b.a; });
    for (auto &q : bf.readChunks())
    {
        cout << "read -> " << q.a << endl;
    }
    Item a = {.a = 9};
    auto pos = bf.find(a);
    cout << "pos = " << pos << endl;
    if (pos != -1)
    {
        Item q;
        bf.readChunk(q, pos);
        cout << q.a << endl;
    }

    return 0;
}