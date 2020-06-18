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

int main(int argc, char** argv)
{
    int c = 5;
    if (argc > 1){
        c = atoi(argv[1]);
    }
    BinaryFile<Header, Item> bf("binary.bin");
    bf.writeHeader({.version = 2});
    for (int i = 0; i < c; ++i)
    {
        bf.writeChunk({.a = i});
    }
    cout << "count = " << bf.count() << endl;
    return 0;
}