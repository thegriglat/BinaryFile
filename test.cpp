#include "BinaryFile.hpp"

#include <iostream>
#include <string>

using namespace std;

struct Header
{
    int version = 2;
};
struct Item
{
    int a;
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
    for (int i = 1200; i < c + 1200; ++i)
    {
        bf.writeChunk({.a = i});
    }
    cout << "### WRITE END ###" << endl;
    cout << "count = " << bf.count() << endl;

    for (int i = 0; i < bf.count(); ++i)
    {
        Item q;
        bf.readChunk(q);
        if (q.a != i + 1200)
        {
            cout << "TOO BAD!" << endl;
        }
    }
    return 0;
}