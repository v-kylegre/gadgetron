
#include "GadgetronClientDependencyQueryReader.h"

#include <fstream>
#include <iostream>

#include "GadgetronClientException.h"

GadgetronClientDependencyQueryReader::GadgetronClientDependencyQueryReader(std::string filename) : number_of_calls_(0) , filename_(filename)
{

}

GadgetronClientDependencyQueryReader::~GadgetronClientDependencyQueryReader()
{

}

void GadgetronClientDependencyQueryReader::read(boost::asio::ip::tcp::socket* stream)
{
    size_t recv_count = 0;

    typedef unsigned long long size_t_type;
    size_t_type len(0);
    boost::asio::read(*stream, boost::asio::buffer(&len, sizeof(size_t_type)));

    char* buf = NULL;
    try
    {
        buf = new char[len];
        memset(buf, '\0', len);
        memcpy(buf, &len, sizeof(size_t_type));
    } catch (std::runtime_error &err)
    {
        std::cerr << "DependencyQueryReader, failed to allocate buffer" << std::endl;
        throw;
    }


    if (boost::asio::read(*stream, boost::asio::buffer(buf, len)) != len)
    {
        delete [] buf;
        throw GadgetronClientException("Incorrect number of bytes read for dependency query");
    }

    std::ofstream outfile;
    outfile.open (filename_.c_str(), std::ios::out|std::ios::binary);

    if (outfile.good())
    {
        outfile.write(buf, len);
        outfile.close();
        number_of_calls_++;
    } else
    {
        delete[] buf;
        throw GadgetronClientException("Unable to write dependency query to file");
    }

    delete[] buf;
}
