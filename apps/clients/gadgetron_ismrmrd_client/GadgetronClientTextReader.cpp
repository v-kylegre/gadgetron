#include "GadgetronClientTextReader.h"

#include <iostream>

#include "GadgetronClientException.h"


GadgetronClientTextReader::GadgetronClientTextReader()
{

}

GadgetronClientTextReader::~GadgetronClientTextReader()
{

}

void GadgetronClientTextReader::read(boost::asio::ip::tcp::socket* stream)
{
    size_t recv_count = 0;

    typedef unsigned long long size_t_type;
    uint32_t len(0);
    boost::asio::read(*stream, boost::asio::buffer(&len, sizeof(uint32_t)));


    char* buf = NULL;
    try {
        buf = new char[len+1];
        memset(buf, '\0', len+1);
    } catch (std::runtime_error &err) {
        std::cerr << "TextReader, failed to allocate buffer" << std::endl;
        throw;
    }

    if (boost::asio::read(*stream, boost::asio::buffer(buf, len)) != len)
    {
        delete [] buf;
        throw GadgetronClientException("Incorrect number of bytes read for dependency query");
    }

    std::string s(buf);
    std::cout << s;
    delete[] buf;
}
