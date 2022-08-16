#include "GadgetronClientResponseReader.h"

#include <iostream>

void GadgetronClientResponseReader::read(boost::asio::ip::tcp::socket *stream)
{
    uint64_t correlation_id = 0;
    uint64_t response_length = 0;

    boost::asio::read(*stream, boost::asio::buffer(&correlation_id, sizeof(correlation_id)));
    boost::asio::read(*stream, boost::asio::buffer(&response_length, sizeof(response_length)));

    std::vector<char> response(response_length + 1,0);

    boost::asio::read(*stream, boost::asio::buffer(response.data(),response_length));

    std::cout << response.data() << std::endl;
}
