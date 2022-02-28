/*****************************************
*  Standalone ISMRMRD Gadgetron Client
*
* Author: Michael S. Hansen
*
* Dependencies: ISMRMRD and Boost
*
*****************************************/

//TODO:
// -Blobs (for DICOM image support)
//  - First implementation is in, but testing needed
// -NIFTI and Analyze output
// -Check on potential threading problem with asio socket
//    - having and reading and writing thread is supposedly not safe, but seems to work here
// -Static linking for standalone executable.

#include <boost/program_options.hpp>
#include <boost/asio.hpp>

#include <ismrmrd/ismrmrd.h>
#include <ismrmrd/dataset.h>
#include <ismrmrd/meta.h>
#include <ismrmrd/xml.h>
#include <ismrmrd/waveform.h>

#include <streambuf>
#include <time.h>
#include <iomanip>
#include <iostream>
#include <exception>
#include <map>
#include <thread>
#include <chrono>
#include <condition_variable>

#include "NHLBICompression.h"
#include "GadgetronTimer.h"
#include "GadgetronClientResponseReader.h"
#include "GadgetronClientException.h"
#include "GadgetronClientTextReader.h"
#include "GadgetronClientDependencyQueryReader.h"
#include "GadgetronClientImageMessageReader.h"
#include "GadgetronClientBlobMessageReader.h"
#include "GadgetronClientAnalyzeImageMessageReader.h"
#include "GadgetronClientQueryToStringReader.h"
#include "GadgetronClientConnector.h"
#include "GadgetronMessageDefs.h"

#if defined GADGETRON_COMPRESSION_ZFP
#include <zfp.h>
#endif

namespace po = boost::program_options;

std::mutex mtx;

namespace
{

std::string get_date_time_string()
{
    time_t rawtime;
    struct tm * timeinfo;
    time ( &rawtime );
    timeinfo = localtime ( &rawtime );

    std::stringstream str;
    str << timeinfo->tm_year+1900 << "-"
        << std::setw(2) << std::setfill('0') << timeinfo->tm_mon+1 << "-"
        << std::setw(2) << std::setfill('0') << timeinfo->tm_mday << " "
        << std::setw(2) << std::setfill('0') << timeinfo->tm_hour << ":"
        << std::setw(2) << std::setfill('0') << timeinfo->tm_min << ":"
        << std::setw(2) << std::setfill('0') << timeinfo->tm_sec;

    std::string ret = str.str();

    return ret;
}

NoiseStatistics get_noise_statistics(std::string dependency_name, std::string host_name, std::string port, unsigned int timeout_ms)
{
    GadgetronClientConnector con;
    con.set_timeout(timeout_ms);
    std::string result;
    NoiseStatistics stat;

    con.register_reader(GADGET_MESSAGE_DEPENDENCY_QUERY, std::make_shared<GadgetronClientQueryToStringReader>(result));

    std::string xml_config;

    xml_config += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    xml_config += "      <gadgetronStreamConfiguration xsi:schemaLocation=\"http://gadgetron.sf.net/gadgetron gadgetron.xsd\"\n";
    xml_config += "        xmlns=\"http://gadgetron.sf.net/gadgetron\"\n";
    xml_config += "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n";
    xml_config += "\n";
    xml_config += "    <writer>\n";
    xml_config += "      <slot>1019</slot>\n";
    xml_config += "      <dll>gadgetron_mricore</dll>\n";
    xml_config += "      <classname>DependencyQueryWriter</classname>\n";
    xml_config += "    </writer>\n";
    xml_config += "\n";
    xml_config += "    <gadget>\n";
    xml_config += "      <name>NoiseSummary</name>\n";
    xml_config += "      <dll>gadgetron_mricore</dll>\n";
    xml_config += "      <classname>NoiseSummaryGadget</classname>\n";
    xml_config += "\n";
    xml_config += "      <property>\n";
    xml_config += "         <name>noise_file</name>\n";
    xml_config += "         <value>" + dependency_name + "</value>\n";
    xml_config += "      </property>\n";
    xml_config += "    </gadget>\n";
    xml_config += "\n";
    xml_config += "</gadgetronStreamConfiguration>\n";

    try {
        con.connect(host_name,port);
        con.send_gadgetron_configuration_script(xml_config);
        con.send_gadgetron_close();
        con.wait();
    } catch (...) {
        std::cerr << "Unable to retrieve noise statistics from server" << std::endl;
        stat.status = false;
    }

    try {
        ISMRMRD::MetaContainer meta;
        ISMRMRD::deserialize(result.c_str(), meta);
        stat.status = meta.as_str("status") == std::string("success");
        stat.channels = meta.as_long("channels");
        stat.sigma_min = meta.as_double("min_sigma");
        stat.sigma_max = meta.as_double("max_sigma");
        stat.sigma_mean = meta.as_double("mean_sigma");
        stat.noise_dwell_time_us = meta.as_double("noise_dwell_time_us");
    } catch (...) {
        stat.status = false;
    }

    return stat;
}

void send_ismrmrd_acq(GadgetronClientConnector& con, ISMRMRD::Acquisition& acq_tmp,
    unsigned int compression_precision, bool use_zfp_compression, float compression_tolerance, NoiseStatistics& noise_stats)
{
    try
    {
        if (compression_precision > 0)
        {
            if (use_zfp_compression) {
                con.send_ismrmrd_zfp_compressed_acquisition_precision(acq_tmp, compression_precision);
            }
            else {
                con.send_ismrmrd_compressed_acquisition_precision(acq_tmp, compression_precision);
            }
        }
        else if (compression_tolerance > 0.0)
        {
            if (use_zfp_compression) {
                con.send_ismrmrd_zfp_compressed_acquisition_tolerance(acq_tmp, compression_tolerance, noise_stats);
            }
            else {
                con.send_ismrmrd_compressed_acquisition_tolerance(acq_tmp, compression_tolerance, noise_stats);
            }
        }
        else
        {
            con.send_ismrmrd_acquisition(acq_tmp);
        }
    }
    catch(...)
    {
        throw GadgetronClientException("send_ismrmrd_acq failed ... ");
    }
}
}

int main(int argc, char **argv)
{
    std::string host_name;
    std::string port;
    std::string in_filename;
    std::string out_filename;
    std::string hdf5_in_group;
    std::string hdf5_out_group;
    std::string config_file;
    std::string config_file_local;
    std::string config_xml_local;
    unsigned int loops;
    unsigned int timeout_ms;
    std::string out_fileformat;
    bool open_input_file = true;
    unsigned int compression_precision = 0;
    float compression_tolerance = 0.0;
    bool use_zfp_compression = false;
    bool verbose = false;
    Gadgetron::GadgetronTimer timer(false);

    po::options_description desc("Allowed options");

    desc.add_options()
        ("help,h", "Produce help message")
        ("query,q", "Dependency query mode")
        ("verbose,v", "Verbose mode")
        ("info,Q", po::value<std::string>(), "Query Gadgetron information")
        ("port,p", po::value<std::string>(&port)->default_value("9002"), "Port")
        ("address,a", po::value<std::string>(&host_name)->default_value("localhost"), "Address (hostname) of Gadgetron host")
        ("filename,f", po::value<std::string>(&in_filename), "Input file")
        ("outfile,o", po::value<std::string>(&out_filename)->default_value("out.h5"), "Output file")
        ("in-group,g", po::value<std::string>(&hdf5_in_group)->default_value("/dataset"), "Input data group")
        ("out-group,G", po::value<std::string>(&hdf5_out_group)->default_value(get_date_time_string()), "Output group name")
        ("config,c", po::value<std::string>(&config_file)->default_value("default.xml"), "Configuration file (remote)")
        ("config-local,C", po::value<std::string>(&config_file_local), "Configuration file (local)")
        ("loops,l", po::value<unsigned int>(&loops)->default_value(1), "Loops")
        ("timeout,t", po::value<unsigned int>(&timeout_ms)->default_value(10000), "Timeout [ms]")
        ("outformat,F", po::value<std::string>(&out_fileformat)->default_value("h5"), "Out format, h5 for hdf5 and hdr for analyze image")
        ("precision,P", po::value<unsigned int>(&compression_precision)->default_value(0), "Compression precision (bits)")
        ("tolerance,T", po::value<float>(&compression_tolerance)->default_value(0.0), "Compression tolerance (fraction of sigma, if no noise stats, assume sigma 1)")
#if defined GADGETRON_COMPRESSION_ZFP
        ("ZFP,Z", po::value<bool>(&use_zfp_compression)->default_value(false), "Use ZFP library for compression");
#endif //GADGETRON_COMPRESSION_ZFP
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (!vm.count("filename") && !vm.count("query") && !vm.count("info")) {
        std::cout << std::endl << std::endl << "\tYou must supply a filename" << std::endl << std::endl;
        std::cout << desc << std::endl;
        return -1;
    }

    if (vm.count("query") || vm.count("info")) {
        open_input_file = false;
    }

    if (vm.count("verbose")) {
        verbose = true;
    }

    if (vm.count("config-local")) {
        std::ifstream t(config_file_local.c_str());
        if (t) {
            //Read in the file.
            config_xml_local = std::string((std::istreambuf_iterator<char>(t)),
                std::istreambuf_iterator<char>());
        } else {
            std::cout << "Unable to read local xml configuration: " << config_file_local  << std::endl;
            return -1;
        }
    }

    if (compression_precision > 0 && compression_tolerance > 0.0) {
       std::cout << "You cannot supply both compression precision (P) and compression tolerance (T) at the same time" << std::endl;
       return -1;
    }

    //Let's check if the files exist:
    std::string hdf5_xml_varname = std::string(hdf5_in_group) + std::string("/xml");
    std::string hdf5_data_varname = std::string(hdf5_in_group) + std::string("/data");
    std::string hdf5_waveform_varname = std::string(hdf5_in_group) + std::string("/waveform");

    //TODO:
    // Add check to see if input file exists

    //Let's open the input file
    std::shared_ptr<ISMRMRD::Dataset> ismrmrd_dataset;
    std::string xml_config;
    if (open_input_file) {
      ismrmrd_dataset = std::shared_ptr<ISMRMRD::Dataset>(new ISMRMRD::Dataset(in_filename.c_str(), hdf5_in_group.c_str(), false));
      // Read the header
      ismrmrd_dataset->readHeader(xml_config);
    }

    if (!vm.count("query") && !vm.count("info")) {
      std::cout << "Gadgetron ISMRMRD client" << std::endl;
      std::cout << "  -- host            :      " << host_name << std::endl;
      std::cout << "  -- port            :      " << port << std::endl;
      std::cout << "  -- hdf5 file  in   :      " << in_filename << std::endl;
      std::cout << "  -- hdf5 group in   :      " << hdf5_in_group << std::endl;
      std::cout << "  -- conf            :      " << config_file << std::endl;
      std::cout << "  -- loop            :      " << loops << std::endl;
      std::cout << "  -- hdf5 file out   :      " << out_filename << std::endl;
      std::cout << "  -- hdf5 group out  :      " << hdf5_out_group << std::endl;
    }


    //Let's figure out if this measurement has dependencies
    NoiseStatistics noise_stats; noise_stats.status = false;
    if (!vm.count("query") && !vm.count("info")) {
        ISMRMRD::IsmrmrdHeader h;
        ISMRMRD::deserialize(xml_config.c_str(),h);

        std::string noise_id;
        if (h.measurementInformation.is_present() &&
            (h.measurementInformation().measurementDependency.size() > 0)) {
            std::cout << "This measurement has dependent measurements" << std::endl;
            for (auto d: h.measurementInformation().measurementDependency) {
                std::cout << "  " << d.dependencyType << " : " << d.measurementID << std::endl;
                if (d.dependencyType == "Noise") {
                    noise_id = d.measurementID;
                }
            }
        }

        if (!noise_id.empty()) {
            std::cout << "Querying the Gadgetron instance for the dependent measurement: " << noise_id << std::endl;
            noise_stats = get_noise_statistics(std::string("GadgetronNoiseCovarianceMatrix_") + noise_id, host_name, port, timeout_ms);
            if (!noise_stats.status) {
                std::cout << "WARNING: Dependent noise measurement not found on Gadgetron server. Was the noise data processed?" << std::endl;
                if (compression_tolerance > 0.0) {
                    std::cout << "  !!!!!! COMPRESSION TOLERANCE LEVEL SPECIFIED, BUT IT IS NOT POSSIBLE TO DETERMINE SIGMA. ASSIMUMING SIGMA == 1 !!!!!!" << std::endl;
                }
            } else {
                std::cout << "Noise level: Min sigma = " << noise_stats.sigma_min << ", Mean sigma = " << noise_stats.sigma_mean << ", Max sigma = " << noise_stats.sigma_max << std::endl;
            }
        }
    }

    GadgetronClientConnector con;
    con.set_timeout(timeout_ms);

    if ( out_fileformat == "hdr" )
    {
        con.register_reader(GADGET_MESSAGE_ISMRMRD_IMAGE, std::shared_ptr<GadgetronClientMessageReader>(new GadgetronClientAnalyzeImageMessageReader(hdf5_out_group)));
    }
    else
    {
        con.register_reader(GADGET_MESSAGE_ISMRMRD_IMAGE, std::shared_ptr<GadgetronClientMessageReader>(
                new GadgetronClientImageMessageReader(out_filename, hdf5_out_group, std::make_unique<std::unique_lock<std::mutex>>(mtx, std::defer_lock))));
    }

    con.register_reader(GADGET_MESSAGE_DICOM_WITHNAME, std::shared_ptr<GadgetronClientMessageReader>(new GadgetronClientBlobMessageReader(std::string(hdf5_out_group), std::string("dcm"))));

    con.register_reader(GADGET_MESSAGE_DEPENDENCY_QUERY, std::shared_ptr<GadgetronClientMessageReader>(new GadgetronClientDependencyQueryReader(std::string(out_filename))));
    con.register_reader(GADGET_MESSAGE_TEXT, std::shared_ptr<GadgetronClientMessageReader>(new GadgetronClientTextReader()));
    con.register_reader(7, std::shared_ptr<GadgetronClientResponseReader>(new GadgetronClientResponseReader()));

    try
    {
        timer.start();
        con.connect(host_name,port);

        if (vm.count("info")) {
            con.send_gadgetron_info_query(vm["info"].as<std::string>());
        }
        else if (vm.count("config-local"))
        {
            con.send_gadgetron_configuration_script(config_xml_local);
        }
        else
        {
            con.send_gadgetron_configuration_file(config_file);
        }

        if (open_input_file)
        {
            con.send_gadgetron_parameters(xml_config);

            uint32_t acquisitions = 0;
            {
                mtx.lock();
                acquisitions = ismrmrd_dataset->getNumberOfAcquisitions();
                mtx.unlock();
            }

            uint32_t waveforms = 0;
            {
                mtx.lock();
                waveforms = ismrmrd_dataset->getNumberOfWaveforms();
                mtx.unlock();
            }

            if(verbose)
            {
                std::cout << "Find " << acquisitions << " ismrmrd acquisitions" << std::endl;
                std::cout << "Find " << waveforms << " ismrmrd waveforms" << std::endl;
            }

            ISMRMRD::Acquisition acq_tmp;
            ISMRMRD::Waveform wav_tmp;

            uint32_t i(0), j(0); // i : index over the acquisition; j : index over the waveform

            if(waveforms>0)
            {
                {
                    std::lock_guard<std::mutex> scoped_lock(mtx);
                    ismrmrd_dataset->readAcquisition(i, acq_tmp);
                }

                {
                    std::lock_guard<std::mutex> scoped_lock(mtx);
                    ismrmrd_dataset->readWaveform(j, wav_tmp);
                }

                while(i<acquisitions && j<waveforms)
                {
                    while(wav_tmp.head.time_stamp < acq_tmp.getHead().acquisition_time_stamp)
                    {
                        con.send_ismrmrd_waveform(wav_tmp);

                        if (verbose)
                        {
                            std::cout << "--> Send out ismrmrd waveform : " << j << " - " << wav_tmp.head.scan_counter
                                << " - " << wav_tmp.head.time_stamp
                                << " - " << wav_tmp.head.channels
                                << " - " << wav_tmp.head.number_of_samples
                                << " - " << wav_tmp.head.waveform_id
                                << std::endl;
                        }

                        j++;

                        if(j<waveforms)
                        {
                            std::lock_guard<std::mutex> scoped_lock(mtx);
                            ismrmrd_dataset->readWaveform(j, wav_tmp);
                        }
                        else
                        {
                            break;
                        }
                    }

                    while (acq_tmp.getHead().acquisition_time_stamp <= wav_tmp.head.time_stamp)
                    {
                        send_ismrmrd_acq(con, acq_tmp, compression_precision, use_zfp_compression, compression_tolerance, noise_stats);

                        if (verbose)
                        {
                            std::cout << "==> Send out ismrmrd acq : " << i << " - " << acq_tmp.getHead().scan_counter << " - " << acq_tmp.getHead().acquisition_time_stamp << std::endl;
                        }

                        i++;

                        if(i<acquisitions)
                        {
                            std::lock_guard<std::mutex> scoped_lock(mtx);
                            ismrmrd_dataset->readAcquisition(i, acq_tmp);
                        }
                        else
                        {
                            break;
                        }
                    }

                    if(j==waveforms && i<acquisitions)
                    {
                        send_ismrmrd_acq(con, acq_tmp, compression_precision, use_zfp_compression, compression_tolerance, noise_stats);

                        for (uint32_t ia=i+1; ia<acquisitions; ia++)
                        {
                            {
                                std::lock_guard<std::mutex> scoped_lock(mtx);
                                ismrmrd_dataset->readAcquisition(ia, acq_tmp);
                            }

                            send_ismrmrd_acq(con, acq_tmp, compression_precision, use_zfp_compression, compression_tolerance, noise_stats);
                        }
                    }

                    if (i==acquisitions && j<waveforms)
                    {
                        con.send_ismrmrd_waveform(wav_tmp);

                        for (uint32_t iw = j + 1; iw<waveforms; iw++)
                        {
                            {
                                std::lock_guard<std::mutex> scoped_lock(mtx);
                                ismrmrd_dataset->readWaveform(iw, wav_tmp);
                            }

                            con.send_ismrmrd_waveform(wav_tmp);
                        }
                    }
                }
            }
            else
            {
                for (i=0; i<acquisitions; i++)
                {
                    {
                        std::lock_guard<std::mutex> scoped_lock(mtx);
                        ismrmrd_dataset->readAcquisition(i, acq_tmp);
                    }

                    send_ismrmrd_acq(con, acq_tmp, compression_precision, use_zfp_compression, compression_tolerance, noise_stats);
                }
            }
        }

        if (compression_precision > 0 || compression_tolerance > 0.0) {
            std::cout << "Compression ratio: " << con.compression_ratio() << std::endl;
        }

        if (verbose) {
            double transmission_time_s = timer.stop()/1e6;
            double transmitted_mb = con.get_bytes_transmitted()/(1024*1024);
            std::cout << "Time sending: " << transmission_time_s << "s" << std::endl;
            std::cout << "Data sent: " << transmitted_mb << "MB" << std::endl;
            std::cout << "Transmission rate: " << transmitted_mb/transmission_time_s << "MB/s" << std::endl;
        }

        con.send_gadgetron_close();
        con.wait();
    }
    catch (std::exception& ex)
    {
        std::cerr << "Error caught: " << ex.what() << std::endl;
        return -1;
    }

    return 0;
}
