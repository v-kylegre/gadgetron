#pragma once


#include <ismrmrd/ismrmrd.h>
#include <ismrmrd/meta.h>
#include <ismrmrd/xml.h>

#include <fstream>

#include "GadgetronClientMessageReader.h"
#include "GadgetronClientException.h"

struct header_key
{
    int sizeof_hdr;
    char data_type[10];
    char db_name[18];
    int extents;
    short int session_error;
    char regular;
    char hkey_un0;
};

struct image_dimension
{
    short int dim[8];
    short int unused8;
    short int unused9;
    short int unused10;
    short int unused11;
    short int unused12;
    short int unused13;
    short int unused14;
    short int datatype;
    short int bitpix;
    short int dim_un0;
    float pixdim[8];
    float vox_offset;
    float funused1;
    float funused2;
    float funused3;
    float cal_max;
    float cal_min;
    float compressed;
    float verified;
    int glmax,glmin;
};

struct data_history
{
    char descrip[80];
    char aux_file[24];
    char orient;
    char originator[10];
    char generated[10];
    char scannum[10];
    char patient_id[10];
    char exp_date[10];
    char exp_time[10];
    char hist_un0[3];
    int views;
    int vols_added;
    int start_field;
    int field_skip;
    int omax, omin;
    int smax, smin;
};

// Analyze75 header has 348 bytes
struct dsr
{
    struct header_key hk;
    struct image_dimension dime;
    struct data_history hist;
};

class IOAnalyze
{
public:

    typedef dsr HeaderType;

    IOAnalyze() {}
    virtual ~IOAnalyze() {}

    template <typename T>
    void array2Header(const std::vector<size_t>& dim, const std::vector<float>& pixelSize, HeaderType& header);
};

class GadgetronClientAnalyzeImageMessageReader : public GadgetronClientMessageReader
{

public:
    GadgetronClientAnalyzeImageMessageReader(const std::string& prefix = std::string("Image"));
    ~GadgetronClientAnalyzeImageMessageReader();

    template <typename T> void read_data_attrib(boost::asio::ip::tcp::socket* stream, const ISMRMRD::ImageHeader& h, ISMRMRD::Image<T>& im);
    virtual void read(boost::asio::ip::tcp::socket* stream);

protected:
    std::string prefix_;
};
