#pragma once 
#include <exception> 


class GadgetronClientException : public std::exception
{

public:
    GadgetronClientException(std::string msg)
        : msg_(msg)
    {

    }

    virtual ~GadgetronClientException() throw() {}

    virtual const char* what() const throw()
    {
        return msg_.c_str();
    }

protected:
    std::string msg_;
};
