/* Copyright 2019-2020 Baumer Optronic */
/*
    This example describes the FIRST STEPS of handling Baumer-GAPI SDK.
    The given source code applies to handling one system, one camera and twelfe images.
    Please see "Baumer-GAPI SDK Programmer's Guide" chapter 5.1 and chapter 5.2
*/
#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#include <vector>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

#if defined(_WIN32)
#   include <direct.h>
#else
#   include <sys/stat.h>
#   include <stdlib.h>
#endif

#if USE_OPENCV
#   include "opencv2/opencv.hpp"
#   include "opencv2/highgui/highgui.hpp"

#if     USE_OCL_COMPONENT == 3
#           include "opencv2/core/ocl.hpp"
#elif   USE_OCL_COMPONENT == 2
#           include "opencv2/ocl/ocl.hpp"
#endif  // USE_OCL_COMPONENT
#endif  // USE_OPENCV


static BGAPI2::Node* nodeEntry(BGAPI2::NodeMap* const pNodeMap, const char* const pName, const bool bRequired,
                               unsigned int* const pMissing = NULL) {
    if (bRequired == false) {
        if (pNodeMap->GetNodePresent(pName) == false) {
            if (pMissing != NULL) {
                *pMissing += 1;
            }
            return NULL;
        }
    }

    return pNodeMap->GetNode(pName);
}

int main() {
    // DECLARATIONS OF VARIABLES
    BGAPI2::SystemList *systemList = NULL;
    BGAPI2::System * pSystem = NULL;
    BGAPI2::String sSystemID;

    BGAPI2::InterfaceList *interfaceList = NULL;
    BGAPI2::Interface * pInterface = NULL;
    BGAPI2::String sInterfaceID;

    BGAPI2::DeviceList *deviceList = NULL;
    BGAPI2::Device * pDevice = NULL;
    BGAPI2::String sDeviceID;

    BGAPI2::DataStreamList *datastreamList = NULL;
    BGAPI2::DataStream * pDataStream = NULL;
    BGAPI2::String sDataStreamID;

    BGAPI2::BufferList *bufferList = NULL;
    BGAPI2::Buffer * pBuffer = NULL;

    std::string sSaveBasePath = "";

#ifdef USE_OPENCV   // OpenCV
    std::vector<int> compression_params;
    /*
    compression_params.push_back(cv::ImwriteFlags::IMWRITE_PNG_COMPRESSION);
    compression_params.push_back(0);
    */
#else   // No OpenCV
    // this part is use if no matching OpenCV found in CMake!
    std::cout << "Without OpenCV buffer images are not saved to files!" << std::endl;
    std::cout << "Availability is checked while CMake creates this project." << std::endl;
    std::cout << "Please install OpenCV (version 2.3 or later) or set 'OpenCV_DIR' to the" << std::endl;
    std::cout << "correct path in the CMakeTests.txt script or as a variable in your environment" << std::endl;
    std::cout << "and run CMake again. " << std::endl;
    std::cout << "######################################" << std::endl << std::endl;
#endif  // USE_OPENCV

    int returncode = 0;

    std::cout << std::endl;
    std::cout << "################################################################" << std::endl;
    std::cout << "# PROGRAMMER'S GUIDE Example 018_ImageCapture_BufferedMode.cpp #" << std::endl;
    std::cout << "################################################################" << std::endl;
    std::cout << std::endl << std::endl;


    std::cout << "SYSTEM LIST" << std::endl;
    std::cout << "###########" << std::endl << std::endl;

    // COUNTING AVAILABLE SYSTEMS (TL producers)
    try {
        systemList = BGAPI2::SystemList::GetInstance();
        systemList->Refresh();
        std::cout << "5.1.2   Detected systems:  " << systemList->size() << std::endl;

        // SYSTEM DEVICE INFORMATION
        for (BGAPI2::SystemList::iterator sysIterator = systemList->begin();
            sysIterator != systemList->end();
            sysIterator++) {
            std::cout << "  5.2.1   System Name:     " << sysIterator->GetFileName() << std::endl;
            std::cout << "          System Type:     " << sysIterator->GetTLType() << std::endl;
            std::cout << "          System Version:  " << sysIterator->GetVersion() << std::endl;
            std::cout << "          System PathName: " << sysIterator->GetPathName() << std::endl << std::endl;
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }


    // OPEN THE FIRST SYSTEM IN THE LIST WITH A CAMERA CONNECTED
    try {
        for (BGAPI2::SystemList::iterator sysIterator = systemList->begin();
            sysIterator != systemList->end();
            sysIterator++) {
            std::cout << "SYSTEM" << std::endl;
            std::cout << "######" << std::endl << std::endl;

            try {
                sysIterator->Open();
                std::cout << "5.1.3   Open next system " << std::endl;
                std::cout << "  5.2.1   System Name:     " << sysIterator->GetFileName() << std::endl;
                std::cout << "          System Type:     " << sysIterator->GetTLType() << std::endl;
                std::cout << "          System Version:  " << sysIterator->GetVersion() << std::endl;
                std::cout << "          System PathName: " << sysIterator->GetPathName() << std::endl
                    << std::endl;
                sSystemID = sysIterator->GetID();
                std::cout << "        Opened system - NodeList Information " << std::endl;
                std::cout << "          GenTL Version:   " << sysIterator->GetNode("GenTLVersionMajor")
                    ->GetValue() << "." << sysIterator->GetNode("GenTLVersionMinor")->GetValue()
                    << std::endl << std::endl;

                std::cout << "INTERFACE LIST" << std::endl;
                std::cout << "##############" << std::endl << std::endl;

                try {
                    interfaceList = sysIterator->GetInterfaces();
                    // COUNT AVAILABLE INTERFACES
                    interfaceList->Refresh(100);  // timeout of 100 msec
                    std::cout << "5.1.4   Detected interfaces: " << interfaceList->size() << std::endl;

                    // INTERFACE INFORMATION
                    for (BGAPI2::InterfaceList::iterator ifIterator = interfaceList->begin();
                        ifIterator != interfaceList->end();
                        ifIterator++) {
                        std::cout << "  5.2.2   Interface ID:      " << ifIterator->GetID() << std::endl;
                        std::cout << "          Interface Type:    " << ifIterator->GetTLType() << std::endl;
                        std::cout << "          Interface Name:    " << ifIterator->GetDisplayName()
                            << std::endl << std::endl;
                    }
                }
                catch (BGAPI2::Exceptions::IException& ex) {
                    returncode = (returncode == 0) ? 1 : returncode;
                    std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
                    std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                    std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
                }


                std::cout << "INTERFACE" << std::endl;
                std::cout << "#########" << std::endl << std::endl;

                // OPEN THE NEXT INTERFACE IN THE LIST
                try {
                    for (BGAPI2::InterfaceList::iterator ifIterator = interfaceList->begin();
                        ifIterator != interfaceList->end();
                        ifIterator++) {
                        try {
                            std::cout << "5.1.5   Open interface " << std::endl;
                            std::cout << "  5.2.2   Interface ID:      "
                                << ifIterator->GetID() << std::endl;
                            std::cout << "          Interface Type:    "
                                << ifIterator->GetTLType() << std::endl;
                            std::cout << "          Interface Name:    "
                                << ifIterator->GetDisplayName()
                                << std::endl;
                            ifIterator->Open();
                            // search for any camera is connetced to this interface
                            deviceList = ifIterator->GetDevices();
                            deviceList->Refresh(100);
                            if (deviceList->size() == 0) {
                                std::cout << "5.1.13   Close interface (" << deviceList->size() << " cameras found) "
                                    << std::endl << std::endl;
                                ifIterator->Close();
                            } else {
                                sInterfaceID = ifIterator->GetID();
                                std::cout << "   " << std::endl;
                                std::cout << "        Opened interface - NodeList Information " << std::endl;
                                if (ifIterator->GetTLType() == "GEV") {
                                    std::cout << "          GevInterfaceSubnetIPAddress: " <<
                                        ifIterator->GetNode("GevInterfaceSubnetIPAddress")->GetValue()
                                        << std::endl;
                                    std::cout << "          GevInterfaceSubnetMask:      "
                                        << ifIterator->GetNode("GevInterfaceSubnetMask")->GetValue()
                                        << std::endl;
                                }
                                if (ifIterator->GetTLType() == "U3V") {
                                    // std::cout << "          NodeListCount:     " << ifIterator
                                    // ->GetNodeList()->GetNodeCount() << std::endl;
                                }
                                std::cout << "  " << std::endl;
                                break;
                            }
                        }
                        catch (BGAPI2::Exceptions::ResourceInUseException& ex) {
                            returncode = (returncode == 0) ? 1 : returncode;
                            std::cout << " Interface " << ifIterator->GetID() << " already opened " << std::endl;
                            std::cout << " ResourceInUseException: " << ex.GetErrorDescription() << std::endl;
                        }
                    }
                }
                catch (BGAPI2::Exceptions::IException& ex) {
                    returncode = (returncode == 0) ? 1 : returncode;
                    std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
                    std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                    std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
                }


                // if a camera is connected to the system interface then leave the system loop
                if (sInterfaceID != "") {
                    break;
                }
            }
            catch (BGAPI2::Exceptions::ResourceInUseException& ex) {
                returncode = (returncode == 0) ? 1 : returncode;
                std::cout << " System " << sysIterator->GetID() << " already opened " << std::endl;
                std::cout << " ResourceInUseException: " << ex.GetErrorDescription() << std::endl;
            }
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    if (sSystemID == "") {
        std::cout << " No System found " << std::endl;
        std::cout << std::endl << "End" << std::endl << "Input any number to close the program:";
        int endKey = 0;
        std::cin >> endKey;
        BGAPI2::SystemList::ReleaseInstance();
        return returncode;
    } else {
        pSystem = (*systemList)[sSystemID];
    }


    if (sInterfaceID == "") {
        std::cout << " No camera found " << sInterfaceID << std::endl;
        std::cout << std::endl << "End" << std::endl << "Input any number to close the program:";
        int endKey = 0;
        std::cin >> endKey;
        pSystem->Close();
        BGAPI2::SystemList::ReleaseInstance();
        return returncode;
    } else {
        pInterface = (*interfaceList)[sInterfaceID];
    }


    std::cout << "DEVICE LIST" << std::endl;
    std::cout << "###########" << std::endl << std::endl;

    try {
        // COUNTING AVAILABLE CAMERAS
        deviceList = pInterface->GetDevices();
        deviceList->Refresh(100);
        std::cout << "5.1.6   Detected devices:         " << deviceList->size() << std::endl;

        // DEVICE INFORMATION BEFORE OPENING
        for (BGAPI2::DeviceList::iterator devIterator = deviceList->begin();
            devIterator != deviceList->end();
            devIterator++) {
            std::cout << "  5.2.3   Device DeviceID:        " << devIterator->GetID() << std::endl;
            std::cout << "          Device Model:           " << devIterator->GetModel() << std::endl;
            std::cout << "          Device SerialNumber:    " << devIterator->GetSerialNumber() << std::endl;
            std::cout << "          Device Vendor:          " << devIterator->GetVendor() << std::endl;
            std::cout << "          Device TLType:          " << devIterator->GetTLType() << std::endl;
            std::cout << "          Device AccessStatus:    " << devIterator->GetAccessStatus() << std::endl;
            std::cout << "          Device UserID:          " << devIterator->GetDisplayName()
                << std::endl << std::endl;
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }


    std::cout << "DEVICE" << std::endl;
    std::cout << "######" << std::endl << std::endl;

    // OPEN THE FIRST CAMERA IN THE LIST
    try {
        for (BGAPI2::DeviceList::iterator devIterator = deviceList->begin();
            devIterator != deviceList->end();
            devIterator++) {
            try {
                std::cout << "5.1.7   Open first device " << std::endl;
                std::cout << "          Device DeviceID:        " << devIterator->GetID() << std::endl;
                std::cout << "          Device Model:           " << devIterator->GetModel() << std::endl;
                std::cout << "          Device SerialNumber:    " << devIterator->GetSerialNumber() << std::endl;
                std::cout << "          Device Vendor:          " << devIterator->GetVendor() << std::endl;
                std::cout << "          Device TLType:          " << devIterator->GetTLType() << std::endl;
                std::cout << "          Device AccessStatus:    " << devIterator->GetAccessStatus() << std::endl;
                std::cout << "          Device UserID:          " << devIterator->GetDisplayName()
                    << std::endl << std::endl;
                devIterator->Open();
                sDeviceID = devIterator->GetID();
                std::cout << "        Opened device - RemoteNodeList Information " << std::endl;
                std::cout << "          Device AccessStatus:    " << devIterator->GetAccessStatus()
                    << std::endl;

                // SERIAL NUMBER
                if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceSerialNumber"))
                    std::cout << "          DeviceSerialNumber:     " <<
                        devIterator->GetRemoteNode("DeviceSerialNumber")->GetValue() << std::endl;
                else if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceID"))
                    std::cout << "          DeviceID (SN):          " <<
                        devIterator->GetRemoteNode("DeviceID")->GetValue() << std::endl;
                else
                    std::cout << "          SerialNumber:           Not Available " << std::endl;

                // DISPLAY DEVICEMANUFACTURERINFO
                if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceManufacturerInfo"))
                    std::cout << "          DeviceManufacturerInfo: " <<
                        devIterator->GetRemoteNode("DeviceManufacturerInfo")->GetValue() << std::endl;

                // DISPLAY DEVICEFIRMWAREVERSION OR DEVICEVERSION
                if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceFirmwareVersion"))
                    std::cout << "          DeviceFirmwareVersion:  " <<
                        devIterator->GetRemoteNode("DeviceFirmwareVersion")->GetValue() << std::endl;
                else if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceVersion"))
                    std::cout << "          DeviceVersion:          " <<
                        devIterator->GetRemoteNode("DeviceVersion")->GetValue() << std::endl;
                else
                    std::cout << "          DeviceVersion:          Not Available " << std::endl;

                if (devIterator->GetTLType() == "GEV") {
                    std::cout << "          GevCCP:                 " <<
                        devIterator->GetRemoteNode("GevCCP")->GetValue() << std::endl;
                    std::cout << "          GevCurrentIPAddress:    " <<
                        devIterator->GetRemoteNode("GevCurrentIPAddress")->GetValue() << std::endl;
                    std::cout << "          GevCurrentSubnetMask:   " <<
                        devIterator->GetRemoteNode("GevCurrentSubnetMask")->GetValue() << std::endl;
                }
                std::cout << "  " << std::endl;
                break;
            }
            catch (BGAPI2::Exceptions::ResourceInUseException& ex) {
                returncode = (returncode == 0) ? 1 : returncode;
                std::cout << " Device  " << devIterator->GetID() << " already opened " << std::endl;
                std::cout << " ResourceInUseException: " << ex.GetErrorDescription() << std::endl;
            }
            catch (BGAPI2::Exceptions::AccessDeniedException& ex) {
                returncode = (returncode == 0) ? 1 : returncode;
                std::cout << " Device  " << devIterator->GetID() << " already opened " << std::endl;
                std::cout << " AccessDeniedException " << ex.GetErrorDescription() << std::endl;
            }
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    if (sDeviceID == "") {
        std::cout << " No Device found " << sDeviceID << std::endl;
        std::cout << std::endl << "End" << std::endl << "Input any number to close the program:";
        int endKey = 0;
        std::cin >> endKey;
        pInterface->Close();
        pSystem->Close();
        BGAPI2::SystemList::ReleaseInstance();
        return returncode;
    } else {
        pDevice = (*deviceList)[sDeviceID];
    }


    BGAPI2::NodeMap* pDeviceRemoteNodeMap = pDevice->GetRemoteNodeList();

    std::cout << "DEVICE PARAMETER SETUP" << std::endl;
    std::cout << "######################" << std::endl << std::endl;

    try {
        // SET TRIGGER MODE OFF (FreeRun)
        pDeviceRemoteNodeMap->GetNode("TriggerMode")->SetString("Off");
        std::cout << "         TriggerMode:             " << pDeviceRemoteNodeMap->GetNode("TriggerMode")->GetValue()
            << std::endl;
        std::cout << std::endl;

        BGAPI2::String sExposureNodeName = "";
        if (pDeviceRemoteNodeMap->GetNodePresent("ExposureTime")) {
            sExposureNodeName = "ExposureTime";
        } else if (pDeviceRemoteNodeMap->GetNodePresent("ExposureTimeAbs")) {
            sExposureNodeName = "ExposureTimeAbs";
        }
        BGAPI2::Node* pDeviceExposureTime = pDevice->GetRemoteNode(sExposureNodeName);

        // EXPOSURE TIME
        bo_double fExposureTime = 0;
        bo_double fExposureTimeMin = 0;
        bo_double fExposureTimeMax = 0;

        // get current value and limits
        fExposureTime = pDeviceExposureTime->GetDouble();
        fExposureTimeMin = pDeviceExposureTime->GetDoubleMin();
        fExposureTimeMax = pDeviceExposureTime->GetDoubleMax();

        std::cout << "          current value:          " << std::fixed << std::setprecision(0) << fExposureTime
            << std::endl;
        std::cout << "          possible value range:   " << std::fixed << std::setprecision(0) << fExposureTimeMin
            << " to " << fExposureTimeMax << std::endl;

        // set new exposure value to 50000 usec
        bo_double exposurevalue = 50000;

        // check new value is within range
        if (exposurevalue < fExposureTimeMin)
            exposurevalue = fExposureTimeMin;

        if (exposurevalue > fExposureTimeMax)
            exposurevalue = fExposureTimeMax;

        pDeviceExposureTime->SetDouble(exposurevalue);

        // recheck new exposure is set
        std::cout << "          set value to:           " << std::fixed << std::setprecision(0)
            << pDeviceExposureTime->GetDouble() << std::endl << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }


    std::cout << "DATA STREAM LIST" << std::endl;
    std::cout << "################" << std::endl << std::endl;

    try {
        // COUNTING AVAILABLE DATASTREAMS
        datastreamList = pDevice->GetDataStreams();
        datastreamList->Refresh();
        std::cout << "5.1.8   Detected datastreams:     " << datastreamList->size() << std::endl;

        // DATASTREAM INFORMATION BEFORE OPENING
        for (BGAPI2::DataStreamList::iterator dstIterator = datastreamList->begin();
            dstIterator != datastreamList->end();
            dstIterator++) {
            std::cout << "  5.2.4   DataStream ID:          " << dstIterator->GetID() << std::endl << std::endl;
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }


    std::cout << "DATA STREAM" << std::endl;
    std::cout << "###########" << std::endl << std::endl;

    // OPEN THE FIRST DATASTREAM IN THE LIST
    try {
        if (datastreamList->size() > 0) {
            pDataStream = (*datastreamList)[0];
            std::cout << "5.1.9   Open first datastream " << std::endl;
            std::cout << "          DataStream ID:          " << pDataStream->GetID() << std::endl << std::endl;
            pDataStream->Open();
            sDataStreamID = pDataStream->GetID();
            std::cout << "        Opened datastream - NodeList Information " << std::endl;
            std::cout << "          StreamAnnounceBufferMinimum:  " <<
                pDataStream->GetNode("StreamAnnounceBufferMinimum")->GetValue() << std::endl;
            if (pDataStream->GetTLType() == "GEV") {
                std::cout << "          StreamDriverModel:            " <<
                    pDataStream->GetNode("StreamDriverModel")->GetValue() << std::endl;
            }
            std::cout << "  " << std::endl;
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    if (sDataStreamID == "") {
        std::cout << " No DataStream found" << std::endl;
        std::cout << std::endl << "End" << std::endl << "Input any number to close the program:";
        int endKey = 0;
        std::cin >> endKey;
        pDevice->Close();
        pInterface->Close();
        pSystem->Close();
        BGAPI2::SystemList::ReleaseInstance();
        return returncode;
    }


    std::cout << "BUFFER LIST" << std::endl;
    std::cout << "###########" << std::endl << std::endl;

    struct MemoryPartData {
        std::string sPart;
        const char* const pMode;
        const unsigned int blocks;
        const unsigned int previewRatio;
    };

    MemoryPartData memoryPart[] = {
        { "", "Cyclic", 10, 2 },
        { "", "Cyclic", 50, 10 },
        { "", "Cyclic", 50, 0 },
        { "", "Cyclic", 20, 5 },
    };

    unsigned int bufferCount = 4;
    for (unsigned int i = 0; i < sizeof(memoryPart) / sizeof(memoryPart[0]); i++) {
        if (bufferCount < memoryPart[i].blocks) {
            bufferCount = memoryPart[i].blocks;
        }
    }

    try {
        // BufferList
        bufferList = pDataStream->GetBufferList();

        // 4 buffers using internal buffer mode
        for (unsigned int i = 0; i < bufferCount; i++) {
            pBuffer = new BGAPI2::Buffer();
            bufferList->Add(pBuffer);
        }
        std::cout << "5.1.10   Announced buffers:       " << bufferList->GetAnnouncedCount() << " using "
            << pBuffer->GetMemSize() * bufferList->GetAnnouncedCount() << " [bytes]" << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    try {
        for (BGAPI2::BufferList::iterator bufIterator = bufferList->begin();
            bufIterator != bufferList->end();
            bufIterator++) {
            bufIterator->QueueBuffer();
        }
        std::cout << "5.1.11   Queued buffers:          " << bufferList->GetQueuedCount() << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }


    // MEMORY PART EXAMPLE
    try {
        BGAPI2::Node* pDeviceAcquisitionStart = NULL;
        BGAPI2::Node* pDeviceAcquisitionStop = NULL;
        BGAPI2::Node* pDeviceAcquisitionAbort = NULL;

        BGAPI2::Node* pMemoryActivePart = NULL;
        BGAPI2::Node* pMemoryMode = NULL;
        BGAPI2::Node* pMemoryPartMode = NULL;
        BGAPI2::Node* pMemoryPartBlocks = NULL;
        BGAPI2::Node* pMemoryPartPreviewRatio = NULL;
        BGAPI2::Node* pMemoryPartSelector = NULL;
        BGAPI2::Node* pMemoryPartFilledBlocks = NULL;
        BGAPI2::Node* pMemoryPartIncrementSource = NULL;
        BGAPI2::Node* pMemoryPartIncrementSoftware = NULL;

        BGAPI2::Node* pTransferSelector = NULL;
        BGAPI2::Node* pTransferStart = NULL;
        BGAPI2::Node* pTransferStop = NULL;

        unsigned int memoryPartNodeMissing = 0;
        unsigned int transferNodeMissing = 0;

        try {
            pDeviceAcquisitionStart = nodeEntry(pDeviceRemoteNodeMap, "AcquisitionStart", true);
            pDeviceAcquisitionStop = nodeEntry(pDeviceRemoteNodeMap, "AcquisitionStop", true);
            pDeviceAcquisitionAbort = nodeEntry(pDeviceRemoteNodeMap, "AcquisitionAbort", false);

            pMemoryActivePart = nodeEntry(pDeviceRemoteNodeMap, "MemoryActivePart", false, &memoryPartNodeMissing);
            pMemoryMode = nodeEntry(pDeviceRemoteNodeMap, "MemoryMode", false, &memoryPartNodeMissing);
            pMemoryPartMode = nodeEntry(pDeviceRemoteNodeMap, "MemoryPartMode", false, &memoryPartNodeMissing);
            pMemoryPartBlocks = nodeEntry(pDeviceRemoteNodeMap, "MemoryPartBlocks", false, &memoryPartNodeMissing);
            pMemoryPartFilledBlocks = nodeEntry(pDeviceRemoteNodeMap, "MemoryPartFilledBlocks", false,
                &memoryPartNodeMissing);
            pMemoryPartIncrementSoftware = nodeEntry(pDeviceRemoteNodeMap, "MemoryPartIncrementSoftware", false,
                &memoryPartNodeMissing);
            pMemoryPartIncrementSource = nodeEntry(pDeviceRemoteNodeMap, "MemoryPartIncrementSource", false,
                &memoryPartNodeMissing);
            pMemoryPartPreviewRatio = nodeEntry(pDeviceRemoteNodeMap, "MemoryPartPreviewRatio", false,
                &memoryPartNodeMissing);
            pMemoryPartSelector = nodeEntry(pDeviceRemoteNodeMap, "MemoryPartSelector", false, &memoryPartNodeMissing);

            pTransferSelector = nodeEntry(pDeviceRemoteNodeMap, "TransferSelector", false, &transferNodeMissing);
            pTransferStart = nodeEntry(pDeviceRemoteNodeMap, "TransferStart", false, &transferNodeMissing);
            pTransferStop = nodeEntry(pDeviceRemoteNodeMap, "TransferStop", false, &transferNodeMissing);
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }

        if ((memoryPartNodeMissing > 0) || (transferNodeMissing > 0)) {
            std::cout << std::endl;
            std::cout << "#####################################" << std::endl;
            std::cout << "# Memory part feature not available #" << std::endl;
            std::cout << "#####################################" << std::endl << std::endl;
        } else if (returncode == 0) {
            std::cout << std::endl;
            std::cout << "CONFIGURE MEMORY PARTS" << std::endl;
            std::cout << "######################" << std::endl << std::endl;

            // STOP IMAGE ACQUISITION
            pDataStream->StopAcquisition();
            pDeviceAcquisitionStop->Execute();

            // BufferList
            BGAPI2::BufferList* pBufferList = pDataStream->GetBufferList();
            pBufferList->FlushAllToInputQueue();

            // STOP TRANSFER
            BGAPI2::NodeMap* pNodeList = pTransferSelector->GetEnumNodeList();
            if (pNodeList != NULL) {
                bo_uint64 count = pNodeList->GetNodeCount();
                for (bo_uint64 i = 0; i < count; i++) {
                    BGAPI2::Node* pNode = pNodeList->GetNodeByIndex(i);
                    std::string sName = pNode->GetValue().get();
                    if (sName != "") {
                        pTransferSelector->SetValue(sName.c_str());
                        pTransferStop->Execute();
                    }
                }
            }

            // ENTER CONFIG MODE
            pMemoryMode->SetValue("Config");

            pNodeList = pMemoryPartSelector->GetEnumNodeList();
            if (pNodeList != NULL) {
                bo_uint64 count = pNodeList->GetNodeCount();
                for (bo_uint64 i = 0; i < count; i++) {
                    BGAPI2::Node* pNode = pNodeList->GetNodeByIndex(i);
                    std::string sName = pNode->GetValue().get();
                    std::cout << "Configure " << sName.c_str() << std::endl;

                    pMemoryPartSelector->SetValue(sName.c_str());
                    if (i < (sizeof(memoryPart) / sizeof(memoryPart[0]))) {
                        memoryPart[i].sPart = sName;

                        pMemoryPartMode->SetValue(memoryPart[i].pMode);
                        pMemoryPartBlocks->SetInt(memoryPart[i].blocks);
                        pMemoryPartPreviewRatio->SetInt(memoryPart[i].previewRatio);
                    } else {
                        pMemoryPartBlocks->SetInt(0);
                        pMemoryPartPreviewRatio->SetInt(0);
                    }

                    std::cout << " Mode:    " << pMemoryPartMode->GetValue().get() << std::endl;
                    std::cout << " Blocks:  " << pMemoryPartBlocks->GetInt() << std::endl;
                    std::cout << " Preview: " << pMemoryPartPreviewRatio->GetInt() << std::endl << std::endl;
                }
            }

            // SET MEMORY PART SWITCHING TO SOFTWARE INCREMENT
            pMemoryPartIncrementSource->SetValue("Software");


            std::cout << std::endl;
            std::cout << "CAMERA START" << std::endl;
            std::cout << "############" << std::endl << std::endl;

            // ACTIVATE MEMORY PARTS
            pMemoryMode->SetValue("Active");

            // START DATASTREAM ACQUISITION
            try {
                pDataStream->StartAcquisitionContinuous();
                std::cout << "5.1.12   DataStream started " << std::endl;
            }
            catch (BGAPI2::Exceptions::IException& ex) {
                returncode = (returncode == 0) ? 1 : returncode;
                std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
                std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
            }

            // START CAMERA
            try {
                std::cout << "5.1.12   " << pDevice->GetModel() << " started " << std::endl;
                pDeviceAcquisitionStart->Execute();
            }
            catch (BGAPI2::Exceptions::IException& ex) {
                returncode = (returncode == 0) ? 1 : returncode;
                std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
                std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
            }

            // CAPTURE MEMORY PARTS
            std::cout << std::endl;
            std::cout << "CAPTURE MEMORY PARTS" << std::endl;
            std::cout << "####################" << std::endl << std::endl;

            try {
                // START PREVIEW TRANSFER
                pTransferSelector->SetValue("Stream0");
                pTransferStart->Execute();

                for (unsigned int k = 0; k < sizeof(memoryPart) / sizeof(memoryPart[0]); k++) {
                    std::cout << "Capture memory part: " << memoryPart[k].sPart.c_str() << std::endl;
                    BGAPI2::String sActiveMemoryPart = pMemoryActivePart->GetString();
                    std::cout << "Active part: " << sActiveMemoryPart.get() << std::endl;
                    if (memoryPart[k].sPart != sActiveMemoryPart.get()) {
                        returncode = (returncode == 0) ? 1 : returncode;
                        std::cout << " Error - wrong memory part";
                    }

                    pMemoryPartSelector->SetValue(memoryPart[k].sPart.c_str());
                    bo_uint64 blocks = pMemoryPartBlocks->GetInt();
                    bo_uint64 previewRatio = pMemoryPartPreviewRatio->GetInt();

                    unsigned int retry = 0;
                    unsigned int retryMax = 1;
                    bo_uint64 previousFilledBlocks = 0;
                    bo_uint64 filledBlocks = 0;
                    unsigned int previewCount = 0;

                    while (true) {
                        if (previewRatio == 0) {
                            filledBlocks = pMemoryPartFilledBlocks->GetInt();
                            if (filledBlocks != previousFilledBlocks) {
                                std::cout << " [" << std::setw(3) << filledBlocks << "/" << std::setw(3) << blocks
                                    << "]" << std::endl;
                            }
                        } else {
                            BGAPI2::Buffer* const pBufferFilled = pDataStream->GetFilledBuffer(1000);  // timeout
                            filledBlocks = pMemoryPartFilledBlocks->GetInt();
                            if (pBufferFilled == NULL) {
                                std::cout << "Error: Buffer Timeout after 1000 msec" << std::endl;
                                if (retry >= retryMax) {
                                    break;
                                }
                                retry++;
                            } else {
                                retry = 0;
                                previewCount++;

                                std::cout << " [" << std::setw(3) << filledBlocks << "/" << std::setw(3) << blocks
                                    << "] Preview " << std::setw(3) << previewCount << " - ";
                                if (pBufferFilled->GetIsIncomplete() == true) {
                                    std::cout << "Error: image is incomplete" << std::endl;
                                } else {
                                    std::cout << "image " << std::setw(5) << pBufferFilled->GetFrameID()
                                        << " received in memory address " << std::hex << pBufferFilled->GetMemPtr()
                                        << std::dec << std::endl;
                                }

                                // queue buffer again
                                pBufferFilled->QueueBuffer();
                            }
                        }

                        previousFilledBlocks = filledBlocks;
                        if (filledBlocks >= blocks) {
                            break;
                        }
                    }

                    std::cout << std::endl;
                    // SOFTWARE INCREMENT TO SWITCH TO NEXT PART
                    pMemoryPartIncrementSoftware->Execute();
                }

                // STOP PREVIEW TRANSFER
                pTransferStop->Execute();
            }
            catch (BGAPI2::Exceptions::IException& ex) {
                returncode = (returncode == 0) ? 1 : returncode;
                std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
                std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
            }
            std::cout << " " << std::endl;


            std::cout << "STOP ACQUISITION" << std::endl;
            std::cout << "################" << std::endl << std::endl;

            // STOP ACQUISITION
            try {
                if (pDeviceAcquisitionAbort) {
                    pDeviceAcquisitionAbort->Execute();
                    std::cout << "5.1.12   " << pDevice->GetModel() << " aborted " << std::endl;
                }

                pDeviceAcquisitionStop->Execute();
                std::cout << "5.1.12   " << pDevice->GetModel() << " stopped " << std::endl;
                std::cout << std::endl;
            }
            catch (BGAPI2::Exceptions::IException& ex) {
                returncode = (returncode == 0) ? 1 : returncode;
                std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
                std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
            }

            pBufferList->FlushAllToInputQueue();

            // TRANSFER MEMORY PARTS
            std::cout << std::endl;
            std::cout << "TRANSFER MEMORY PARTS" << std::endl;
            std::cout << "#####################" << std::endl << std::endl;

#ifdef USE_OPENCV
            BGAPI2::ImageProcessor* pImageProcessor = new BGAPI2::ImageProcessor();
            bo_uint64 bufferSizeDst = 0;
#endif
            void* pBufferDst = NULL;
            BGAPI2::Image* pImage = NULL;

            for (unsigned int k = 0; k < sizeof(memoryPart) / sizeof(memoryPart[0]); k++) {
                std::cout << "Transfer memory part: " << memoryPart[k].sPart.c_str() << std::endl;

                std::string sPath = sSaveBasePath + memoryPart[k].sPart + "/";
#ifdef USE_OPENCV
#if defined(_WIN32)
                _mkdir(sPath.c_str());
#else
                mkdir(sPath.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
#endif
#endif
                std::stringstream sStream;
                sStream << "Stream" << (k + 1);
                pTransferSelector->SetValue(sStream.str().c_str());

                pTransferStart->Execute();

                unsigned int retry = 0;
                unsigned int retryMax = 0;
                bo_int64 count = 0;
                while (true) {
                    pBuffer = pDataStream->GetFilledBuffer(1000);
                    if (pBuffer == NULL) {
                        std::cout << "Error: Buffer Timeout after 1000 msec" << std::endl;
                        if (retry >= retryMax) {
                            break;
                        }
                        retry++;
                        continue;
                    }

                    retry = 0;
                    count++;

                    std::cout << " Transfer " << std::setw(3) << count << " - ";
                    if (pBuffer->GetIsIncomplete() == true) {
                        std::cout << "Error: image is incomplete" << std::endl;
                    } else {
                        void* pRawBuffer = pBuffer->GetMemPtr();
                        std::cout << "image " << std::setw(5) << pBuffer->GetFrameID()
                            << " received in memory address " << std::hex << pRawBuffer << std::dec << std::endl;

#if USE_OPENCV  // OpenCV
                        if (pRawBuffer != NULL) {
                            sStream.str("");
                            sStream << sPath << "Img_" << std::setw(3) << std::setfill('0') << count << ".jpg";
                            std::string sFilename = sStream.str();
                            BGAPI2::String sPixelFormat = pBuffer->GetPixelFormat();
                            const int width = static_cast<int>(pBuffer->GetWidth());
                            const int height = static_cast<int>(pBuffer->GetHeight());
                            if (sPixelFormat == "Mono8") {  // Save image as jpeg without conversion
                                cv::Mat img(width, height, CV_8UC1, pRawBuffer, cv::Mat::AUTO_STEP);
                                if (cv::imwrite(sFilename, img, compression_params) == false) {
                                    std::cout << "Error while saving '" << sFilename << "'" << std::endl;
                                }
                            } else if (pImageProcessor != NULL) {  // Convert image to BGR8 and save as jpeg
                                if (pImage == NULL) {
                                    // Create new BGAPI2::Image object
                                    pImage = pImageProcessor->CreateImage(width, height, sPixelFormat, pRawBuffer,
                                        pBuffer->GetMemSize());
                                } else {
                                    // Reinitialise existing BGAPI2:::Image object
                                    pImage->Init(width, height, sPixelFormat, pRawBuffer, pBuffer->GetMemSize());
                                }

                                const bo_uint64 size = pImage->GetTransformBufferLength("BGR8");
                                if (bufferSizeDst < size) {
                                    // destination buffer too small
                                    pBufferDst = realloc(pBufferDst, size);
                                    bufferSizeDst = size;
                                }

                                // Convert to BGR8
                                pImageProcessor->TransformImageToBuffer(pImage, "BGR8", pBufferDst, bufferSizeDst);

                                // Save image as jpeg
                                cv::Mat img(width, height, CV_8UC3, pBufferDst, cv::Mat::AUTO_STEP);
                                if (cv::imwrite(sFilename, img) == false) {
                                    std::cout << "error while saving '" << sFilename << "'" << std::endl;
                                }
                            }
                        }
#endif  // USE_OPENCV
                    }

                    pBuffer->QueueBuffer();

                    if (count >= memoryPart[k].blocks) {
                        break;
                    }
                }

                pMemoryPartSelector->SetValue(memoryPart[k].sPart.c_str());
                if (count != pMemoryPartBlocks->GetInt()) {
                    returncode = (returncode == 0) ? 1 : returncode;
                    std::cout << "Error - transfered: " << count << " expected: " << pMemoryPartBlocks->GetInt()
                        << std::endl;
                }

                std::cout << std::endl;

                pTransferStop->Execute();

                pBufferList->FlushAllToInputQueue();
            }

            if (pImage != NULL) {
                pImage->Release();
                pImage = NULL;
            }

            if (pBufferDst != NULL) {
                free(pBufferDst);
                pBufferDst = NULL;
            }

#ifdef USE_OPENCV
            if (pImageProcessor != NULL) {
                delete pImageProcessor;
                pImageProcessor = NULL;
            }
#endif

            // SET CAMERA TO LIVE MODE
            pMemoryMode->SetValue("Off");
            pTransferSelector->SetString("Stream0");
            pTransferStart->Execute();
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }


    // STOP DataStream acquisition
    try {
        BGAPI2::NodeMap* pDataStreamNodeList = pDataStream->GetNodeList();
        if (pDataStream->GetTLType() == "GEV") {
            // DataStream Statistic
            std::cout << "         DataStream Statistics " << std::endl;
            std::cout << "           DataBlockComplete:              "
                << pDataStreamNodeList->GetNode("DataBlockComplete")->GetInt() << std::endl;
            std::cout << "           DataBlockInComplete:            "
                << pDataStreamNodeList->GetNode("DataBlockInComplete")->GetInt() << std::endl;
            std::cout << "           DataBlockMissing:               "
                << pDataStreamNodeList->GetNode("DataBlockMissing")->GetInt() << std::endl;
            std::cout << "           PacketResendRequestSingle:      "
                << pDataStreamNodeList->GetNode("PacketResendRequestSingle")->GetInt() << std::endl;
            std::cout << "           PacketResendRequestRange:       "
                << pDataStreamNodeList->GetNode("PacketResendRequestRange")->GetInt() << std::endl;
            std::cout << "           PacketResendReceive:            "
                << pDataStreamNodeList->GetNode("PacketResendReceive")->GetInt() << std::endl;
            std::cout << "           DataBlockDroppedBufferUnderrun: "
                << pDataStreamNodeList->GetNode("DataBlockDroppedBufferUnderrun")->GetInt() << std::endl;
            std::cout << "           Bitrate:                        "
                << pDataStreamNodeList->GetNode("Bitrate")->GetDouble() << std::endl;
            std::cout << "           Throughput:                     "
                << pDataStreamNodeList->GetNode("Throughput")->GetDouble() << std::endl;
            std::cout << std::endl;
        } else if (pDataStream->GetTLType() == "U3V") {
            // DataStream Statistic
            std::cout << "         DataStream Statistics " << std::endl;
            std::cout << "           GoodFrames:            "
                << pDataStreamNodeList->GetNode("GoodFrames")->GetInt() << std::endl;
            std::cout << "           CorruptedFrames:       "
                << pDataStreamNodeList->GetNode("CorruptedFrames")->GetInt() << std::endl;
            std::cout << "           LostFrames:            "
                << pDataStreamNodeList->GetNode("LostFrames")->GetInt() << std::endl;
            std::cout << std::endl;
        }

        // BufferList Information
        std::cout << "         BufferList Information " << std::endl;
        std::cout << "           DeliveredCount:        " << bufferList->GetDeliveredCount() << std::endl;
        std::cout << "           UnderrunCount:         " << bufferList->GetUnderrunCount() << std::endl;
        std::cout << std::endl;

        pDataStream->StopAcquisition();
        std::cout << "5.1.12   DataStream stopped " << std::endl;
        bufferList->DiscardAllBuffers();
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
    std::cout << std::endl;



    std::cout << "RELEASE" << std::endl;
    std::cout << "#######" << std::endl << std::endl;

    // Release buffers
    std::cout << "5.1.13   Releasing the resources " << std::endl;
    try {
        while (bufferList->size() > 0) {
            pBuffer = *(bufferList->begin());
            bufferList->RevokeBuffer(pBuffer);
            delete pBuffer;
        }
        std::cout << "         buffers after revoke:    " << bufferList->size() << std::endl;

        pDataStream->Close();
        pDevice->Close();
        pInterface->Close();
        pSystem->Close();
        BGAPI2::SystemList::ReleaseInstance();
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    std::cout << std::endl;
    std::cout << "End" << std::endl << std::endl;

    std::cout << "Input any number to close the program:";
    int endKey = 0;
    std::cin >> endKey;
    return returncode;
}
