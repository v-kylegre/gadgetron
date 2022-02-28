#include "GadgetronClientQueryToStringReader.h"

#include "GadgetronClientException.h"

GadgetronClientQueryToStringReader::GadgetronClientQueryToStringReader(std::string& result) : result_(result)
{

}

GadgetronClientQueryToStringReader::~GadgetronClientQueryToStringReader()
{

}

void GadgetronClientQueryToStringReader::read(boost::asio::ip::tcp::socket* stream)
{
    size_t recv_count = 0;

    typedef unsigned long long size_t_type;
    size_t_type len(0);
    boost::asio::read(*stream, boost::asio::buffer(&len, sizeof(size_t_type)));

    std::vector<char> temp(len,0);
    if (boost::asio::read(*stream, boost::asio::buffer(temp.data(), len)) != len)
    {
        throw GadgetronClientException("Incorrect number of bytes read for dependency query");
    }
    result_ = std::string(temp.data(),len);
}
