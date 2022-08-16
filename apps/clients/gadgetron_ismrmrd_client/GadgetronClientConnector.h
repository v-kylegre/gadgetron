#pragma once

#include <ismrmrd/dataset.h>
#include <ismrmrd/xml.h>

#include "GadgetronClientMessageReader.h"


struct NoiseStatistics
{
    bool status;
    uint16_t channels;
    float sigma_min;
    float sigma_max;
    float sigma_mean;
    float noise_dwell_time_us;
};

// ----------------------------------------------------------------

class GadgetronClientConnector
{

public:
    GadgetronClientConnector();
    virtual ~GadgetronClientConnector();

    double compression_ratio();

    double get_bytes_transmitted();

    void set_timeout(unsigned int t);
    void read_task();
    void wait();

    void connect(std::string hostname, std::string port);
    void send_gadgetron_close();

    void send_gadgetron_info_query(const std::string &query, uint64_t correlation_id = 0);
    void send_gadgetron_configuration_file(std::string config_xml_name);
    void send_gadgetron_configuration_script(std::string xml_string);
    void send_gadgetron_parameters(std::string xml_string);

    void send_ismrmrd_acquisition(ISMRMRD::Acquisition& acq);
    void send_ismrmrd_compressed_acquisition_precision(ISMRMRD::Acquisition& acq, unsigned int compression_precision);
    void send_ismrmrd_compressed_acquisition_tolerance(ISMRMRD::Acquisition& acq, float compression_tolerance, NoiseStatistics& stat);
    void send_ismrmrd_zfp_compressed_acquisition_precision(ISMRMRD::Acquisition& acq, unsigned int compression_precision);
    void send_ismrmrd_zfp_compressed_acquisition_tolerance(ISMRMRD::Acquisition& acq, float compression_tolerance, NoiseStatistics& stat);
    void send_ismrmrd_waveform(ISMRMRD::Waveform& wav);

    void register_reader(unsigned short slot, std::shared_ptr<GadgetronClientMessageReader> r);

protected:
    typedef std::map<unsigned short, std::shared_ptr<GadgetronClientMessageReader> > maptype;
    GadgetronClientMessageReader* find_reader(unsigned short r);

private:
    boost::asio::io_service io_service_;
    boost::asio::ip::tcp::socket* socket_;
    std::thread reader_thread_;
    maptype readers_;
    unsigned int timeout_ms_;
    double header_bytes_sent_;
    double uncompressed_bytes_sent_;
    double compressed_bytes_sent_;
};
