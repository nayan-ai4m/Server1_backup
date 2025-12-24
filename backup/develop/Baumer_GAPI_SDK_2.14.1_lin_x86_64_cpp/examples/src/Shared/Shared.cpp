#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <set>
#include <vector>
#include <algorithm>

#if defined(__cplusplus) && __cplusplus >= 199711L && _MSC_VER >= 1600 // C++11 and greater 201103L?
#   include <regex>  // C++11
#endif

#include "bgapi2_genicam/bgapi2_genicam.hpp"
#include "HelperFunctions.h"
#include "Shared.h"

static BGAPI2::SystemList *pSystemList = NULL;
static BGAPI2::System * pSystem = NULL;

static BGAPI2::InterfaceList *pInterfaceList = NULL;
static BGAPI2::Interface * pInterface = NULL;
static BGAPI2::String sInterfaceID;

static BGAPI2::DeviceList *pDeviceList = NULL;
static BGAPI2::Device * pDevice = NULL;
static BGAPI2::String sDeviceID;

static BGAPI2::DataStreamList *pDatastreamList = NULL;
static BGAPI2::DataStream * pDataStream = NULL;
static BGAPI2::String sDataStreamID;

static BGAPI2::BufferList *pBufferList = NULL;
static BGAPI2::Buffer * pBuffer = NULL;

#if defined(_WIN32)
static const char c_cPathSeparator = '\\';
#else
static const char c_cPathSeparator = '/';
#endif


// ---------------------------------------------------------------------------------------------------------------------
int initTest(BGAPI2::Device** ppDevice, BGAPI2::DataStream** ppDataStream, std::string sSystemList) {
    // DECLARATIONS OF VARIABLES
    int returncode = 0;
    std::cout << std::endl;
    std::string sSystem = sSystemList;

    // writeHeader1("# " + std::string(strrchr(__FILE__, c_cPathSeparator) ? strrchr(__FILE__, c_cPathSeparator) + 1 :
    //                                __FILE__) + " #");
    // std::cout << std::endl << std::endl;

    writeHeadLine("SYSTEM LIST");

    BGAPI2::Trace::ActivateOutputToFile(true, "bgapi_trace.txt");
    BGAPI2::Trace::Enable(true);

    // COUNTING AVAILABLE SYSTEMS (TL producers)
    try {
        pSystemList = BGAPI2::SystemList::GetInstance();
        pSystemList->Refresh();
        std::cout << "5.1.2   Detected systems:  " << pSystemList->size() << std::endl;

        // SYSTEM DEVICE INFORMATION
        for (BGAPI2::SystemList::iterator sysIterator = pSystemList->begin(); sysIterator != pSystemList->end(); sysIterator++) {
            BGAPI2::System* const pCurrentSystem = *sysIterator;
            std::cout << "  5.2.1   System Name:     " << pCurrentSystem->GetFileName() << std::endl;
            std::cout << "          System Type:     " << pCurrentSystem->GetTLType() << std::endl;
            std::cout << "          System Version:  " << pCurrentSystem->GetVersion() << std::endl;
            std::cout << "          System PathName: " << pCurrentSystem->GetPathName() << std::endl << std::endl;

            if ((pSystem == NULL) && (pCurrentSystem->GetTLType().get() == sSystem)) {
                pSystem = pCurrentSystem;
            }
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    if (pSystem == NULL) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << " No " << sSystem << " System found " << std::endl;
        std::cout << std::endl << "End" << std::endl;
        BGAPI2::SystemList::ReleaseInstance();
        return returncode;
    }

    // OPEN THE "U3V" SYSTEM IN THE LIST WITH A CAMERA CONNECTED
    try {
        std::cout << "SYSTEM" << std::endl;
        std::cout << "######" << std::endl << std::endl;

        try {
            pSystem->Open();
            std::cout << "        Opened system - NodeList Information " << std::endl;
            std::cout << "          GenTL Version:   " << pSystem->GetNode("GenTLVersionMajor")->GetValue() << "." << pSystem->GetNode("GenTLVersionMinor")->GetValue() << std::endl << std::endl;

            std::cout << "INTERFACE LIST" << std::endl;
            std::cout << "##############" << std::endl << std::endl;

            try {
                pInterfaceList = pSystem->GetInterfaces();
                // COUNT AVAILABLE INTERFACES
                pInterfaceList->Refresh(100); // timeout of 100 msec
                std::cout << "5.1.4   Detected interfaces: " << pInterfaceList->size() << std::endl;

                // INTERFACE INFORMATION
                for (BGAPI2::InterfaceList::iterator ifIterator = pInterfaceList->begin(); ifIterator != pInterfaceList->end(); ifIterator++) {
                    std::cout << "  5.2.2   Interface ID:      " << ifIterator->GetID() << std::endl;
                    std::cout << "          Interface Type:    " << ifIterator->GetTLType() << std::endl;
                    std::cout << "          Interface Name:    " << ifIterator->GetDisplayName() << std::endl << std::endl;
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

            // OPEN THE FIRST INTERFACE IN THE LIST
            try {
                for (BGAPI2::InterfaceList::iterator ifIterator = pInterfaceList->begin(); ifIterator != pInterfaceList->end(); ifIterator++) {
                    BGAPI2::Interface* const pCurrentInterface = *ifIterator;
                    try {
                        std::cout << "5.1.5   Open interface " << std::endl;
                        std::cout << "  5.2.2   Interface ID:      " << pCurrentInterface->GetID() << std::endl;
                        std::cout << "          Interface Type:    " << pCurrentInterface->GetTLType() << std::endl;
                        std::cout << "          Interface Name:    " << pCurrentInterface->GetDisplayName() << std::endl;

                        pCurrentInterface->Open();
                        // search for any camera is connetced to this interface
                        pDeviceList = pCurrentInterface->GetDevices();
                        pDeviceList->Refresh(100);
                        if (pDeviceList->size() == 0) {
                            std::cout << "5.1.13   Close interface (" << pDeviceList->size() << " cameras found) " << std::endl << std::endl;
                            pCurrentInterface->Close();
                        } else {
                            sInterfaceID = ifIterator->GetID();

                            std::cout << "   " << std::endl;
                            std::cout << "        Opened interface - NodeList Information " << std::endl;
                            if (pCurrentInterface->GetTLType() == "GEV") {
                                bo_int64 iIpAddress = pInterface->GetNode("GevInterfaceSubnetIPAddress")->GetInt();
                                std::cout << "          GevInterfaceSubnetIPAddress: " << (iIpAddress >> 24) << "."
                                    << ((iIpAddress & 0xffffff) >> 16) << "."
                                    << ((iIpAddress & 0xffff) >> 8) << "."
                                    << (iIpAddress & 0xff) << std::endl;
                                bo_int64 iSubnetMask = pInterface->GetNode("GevInterfaceSubnetMask")->GetInt();
                                std::cout << "          GevInterfaceSubnetMask:      " << (iSubnetMask >> 24) << "."
                                    << ((iSubnetMask & 0xffffff) >> 16) << "."
                                    << ((iSubnetMask & 0xffff) >> 8) << "."
                                    << (iSubnetMask & 0xff) << std::endl;
                            } else if (pCurrentInterface->GetTLType() == "U3V") {
                                // std::cout << "          NodeListCount:     " << pInterface->GetNodeList()->GetNodeCount() << std::endl;
                            }
                            std::cout << "  " << std::endl;
                            break;
                        }
                    }
                    catch (BGAPI2::Exceptions::ResourceInUseException& ex) {
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
        }
        catch (BGAPI2::Exceptions::ResourceInUseException& ex) {
            std::cout << " ResourceInUseException: " << ex.GetErrorDescription() << std::endl;
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    if (sInterfaceID == "") {
        std::cout << " No interface found " << std::endl;
        std::cout << std::endl << "End" << std::endl;
        pSystem->Close();
        BGAPI2::SystemList::ReleaseInstance();
        return returncode;
    } else {
        pInterface = (*pInterfaceList)[sInterfaceID];
    }


    writeHeadLine("DEVICE LIST");
    // std::cout << "DEVICE LIST" << std::endl;
    // std::cout << "###########" << std::endl << std::endl;

    try {
        // COUNTING AVAILABLE CAMERAS
        pDeviceList = pInterface->GetDevices();
        pDeviceList->Refresh(100);
        std::cout << "5.1.6   Detected devices:         " << pDeviceList->size() << std::endl;

        // DEVICE INFORMATION BEFORE OPENING
        for (BGAPI2::DeviceList::iterator devIterator = pDeviceList->begin(); devIterator != pDeviceList->end();
             devIterator++) {
            BGAPI2::Device* const pCurrentDevice = *devIterator;

            std::cout << "  5.2.3   Device DeviceID:        " << pCurrentdevice->GetID() << std::endl;
            std::cout << "          Device Model:           " << pCurrentDevice->GetModel() << std::endl;
            std::cout << "          Device SerialNumber:    " << pCurrentDevice->GetSerialNumber() << std::endl;
            std::cout << "          Device Vendor:          " << pCurrentDevice->GetVendor() << std::endl;
            std::cout << "          Device TLType:          " << pCurrentDevice->GetTLType() << std::endl;
            std::cout << "          Device AccessStatus:    " << pCurrentDevice->GetAccessStatus() << std::endl;
            std::cout << "          Device UserID:          " << pCurrentDevice->GetDisplayName() << std::endl << std::endl;
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
        for (BGAPI2::DeviceList::iterator devIterator = pDeviceList->begin(); devIterator != pDeviceList->end();
             devIterator++) {
            BGAPI2::Device* const pCurrentDevice = *devIterator;
            try {
                std::cout << "5.1.7   Open first device " << std::endl;
                std::cout << "          Device DeviceID:        " << pCurrentdevice->GetID() << std::endl;
                std::cout << "          Device Model:           " << pCurrentDevice->GetModel() << std::endl;
                std::cout << "          Device SerialNumber:    " << pCurrentDevice->GetSerialNumber() << std::endl;
                std::cout << "          Device Vendor:          " << pCurrentDevice->GetVendor() << std::endl;
                std::cout << "          Device TLType:          " << pCurrentDevice->GetTLType() << std::endl;
                std::cout << "          Device AccessStatus:    " << pCurrentDevice->GetAccessStatus() << std::endl;
                std::cout << "          Device UserID:          " << pCurrentDevice->GetDisplayName() << std::endl << std::endl;

                pCurrentDevice->Open();
                BGAPI2::NodeMap* const pDeviceRemoteNodeList = pCurrentDevice->GetRemoteNodeList();

                sDeviceID = devIterator->GetID();
                std::cout << "        Opened device - RemoteNodeList Information " << std::endl;
                std::cout << "          Device AccessStatus:    " << pCurrentDevice->GetAccessStatus() << std::endl;

                // SERIAL NUMBER
                if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_SERIALNUMBER))
                    std::cout << "          DeviceSerialNumber:     " << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_SERIALNUMBER)->GetValue() << std::endl;
                else if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_ID))
                    std::cout << "          DeviceID (SN):          " << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_ID)->GetValue() << std::endl;
                else
                    std::cout << "          SerialNumber:           Not Available " << std::endl;

                // DISPLAY DEVICEMANUFACTURERINFO
                if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_MANUFACTURERINFO))
                    std::cout << "          DeviceManufacturerInfo: " << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_MANUFACTURERINFO)->GetValue() << std::endl;


                // DISPLAY DEVICEFIRMWAREVERSION OR DEVICEVERSION
                if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_FIRMWAREVERSION))
                    std::cout << "          DeviceFirmwareVersion:  " << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_FIRMWAREVERSION)->GetValue() << std::endl;
                else if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_VERSION))
                    std::cout << "          DeviceVersion:          " << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_VERSION)->GetValue() << std::endl;
                else
                    std::cout << "          DeviceVersion:          Not Available " << std::endl;

                if (pCurrentDevice->GetTLType() == "GEV") {
                    const bo_int64 iGevCurrentIpAddress = pCurrentDevice->GetRemoteNode(SFNC_GEV_CURRENTIPADDRESS)->GetInt();
                    const bo_int64 iGevCurrentSubnetMask = pCurrentDevice->GetRemoteNode(SFNC_GEV_CURRENTSUBNETMASK)->GetInt();
                    std::cout << "          GevCCP:                 " << pCurrentDevice->GetRemoteNode(SFNC_GEV_CCP)->GetValue() << std::endl;
                    std::cout << "          GevCurrentIPAddress:    " << ((iGevCurrentIpAddress & 0xff000000) >> 24) << "." << ((iGevCurrentIpAddress & 0x00ff0000) >> 16) << "." << ((iGevCurrentIpAddress & 0x0000ff00) >> 8) << "." << (iGevCurrentIpAddress & 0x0000ff) << std::endl;
                    std::cout << "          GevCurrentSubnetMask:   " << ((iGevCurrentSubnetMask & 0xff000000) >> 24) << "." << ((iGevCurrentSubnetMask & 0x00ff0000) >> 16) << "." << ((iGevCurrentSubnetMask & 0x0000ff00) >> 8) << "." << (iGevCurrentSubnetMask & 0x0000ff) << std::endl;
                }
                std::cout << "  " << std::endl;
                break;
            }
            catch (BGAPI2::Exceptions::ResourceInUseException& ex) {
                std::cout << " Device  " << devIterator->GetID() << " already opened " << std::endl;
                std::cout << " ResourceInUseException: " << ex.GetErrorDescription() << std::endl;
            }
            catch (BGAPI2::Exceptions::AccessDeniedException& ex) {
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
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << " No Device found " << std::endl;
        std::cout << std::endl << "End" << std::endl;
        pInterface->Close();
        pSystem->Close();
        BGAPI2::SystemList::ReleaseInstance();
        return returncode;
    } else {
        pDevice = (*pDeviceList)[sDeviceID];
    }


    std::cout << "DEVICE PARAMETER SETUP" << std::endl;
    std::cout << "######################" << std::endl << std::endl;

    try {
        // EXAMPLE DEVICE REMOTE NODE INTERFACE TYPE -> "IFloat"
        //============================================================

        BGAPI2::Node* const pDeviceExposureTime = pDevice->GetRemoteNode(SFNC_EXPOSURETIME);

        // EXPOSURE TIME
        std::cout << "5.3.2   ExposureTime" << std::endl;
        std::cout << "          description:            " << pDeviceExposureTime->GetDescription() << std::endl;
        std::cout << "          interface type:         " << pDeviceExposureTime->GetInterface() << std::endl;
        bo_double fExposureTime = 0;
        bo_double fExposureTimeMin = 0;
        bo_double fExposureTimeMax = 0;

        // get current value and limits
        fExposureTime = pDeviceExposureTime->GetDouble();
        fExposureTimeMin = pDeviceExposureTime->GetDoubleMin();
        fExposureTimeMax = pDeviceExposureTime->GetDoubleMax();

        std::cout << "          current value:          " << std::fixed << std::setprecision(0) << fExposureTime << std::endl;
        std::cout << "          possible value range:   " << std::fixed << std::setprecision(0) << fExposureTimeMin << " to " << fExposureTimeMax << std::endl;

        // set new exposure value to 20000 usec
        bo_double exposurevalue = 20000;

        // check new value is within range
        if (exposurevalue < fExposureTimeMin)
            exposurevalue = fExposureTimeMin;

        if (exposurevalue > fExposureTimeMax)
            exposurevalue = fExposureTimeMax;

        pDeviceExposureTime->SetDouble(exposurevalue);

        // recheck new exposure is set
        std::cout << "          set value to:           " << std::fixed << std::setprecision(0) << pDeviceExposureTime->GetDouble() << std::endl << std::endl;

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
        pDatastreamList = pDevice->GetDataStreams();
        pDatastreamList->Refresh();
        std::cout << "5.1.8   Detected datastreams:     " << pDatastreamList->size() << std::endl;

        // DATASTREAM INFORMATION BEFORE OPENING
        for (BGAPI2::DataStreamList::iterator dstIterator = pDatastreamList->begin();
             dstIterator != pDatastreamList->end(); dstIterator++) {
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
        if (pDatastreamList->size() > 0) {
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
        std::cout << std::endl << "End" << std::endl;
        pDevice->Close();
        pInterface->Close();
        pSystem->Close();
        BGAPI2::SystemList::ReleaseInstance();
        return returncode;
    }

    std::cout << "BUFFER LIST" << std::endl;
    std::cout << "###########" << std::endl << std::endl;

    try {
        // BufferList
        pBufferList = pDataStream->GetBufferList();
        // 4 buffers using internal buffer mode
        for (int i = 0; i < 50; i++) {
            pBuffer = new BGAPI2::Buffer();
            pBufferList->Add(pBuffer);
        }
        std::cout << "5.1.10   Announced buffers:       " << pBufferList->GetAnnouncedCount() << " using " << pBuffer->GetMemSize() * pBufferList->GetAnnouncedCount() << " [bytes]" << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    try {
        for (BGAPI2::BufferList::iterator bufIterator = pBufferList->begin(); bufIterator != pBufferList->end();
             bufIterator++) {
            bufIterator->QueueBuffer();
        }
        std::cout << "5.1.11   Queued buffers:          " << pBufferList->GetQueuedCount() << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
    std::cout << " " << std::endl;

    std::cout << "SUPPORTED DEVICE PIXEL FORMAT" << std::endl;
    std::cout << "#############################" << std::endl << std::endl;
    std::set<BGAPI2::String> devicePixelFormat;
    BGAPI2::Node* const pDevicePixelFormat = pDevice->GetRemoteNode(SFNC_PIXELFORMAT);
    BGAPI2::NodeMap* pNodeMap = pDevicePixelFormat->GetEnumNodeList();
    bo_uint64 count = pNodeMap->GetNodeCount();
    for (bo_uint64 i = 0; i < count; i++) {
        try {
            BGAPI2::Node* pNode = pNodeMap->GetNodeByIndex(i);
            if ((pNode->GetImplemented()) && (pNode->GetAvailable())) {
                BGAPI2::String sPixelFormat = pNode->GetValue();
                devicePixelFormat.insert(sPixelFormat);
                std::cout << sPixelFormat << std::endl;
            }
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }
    }
    std::cout << std::endl;

    *ppDevice = pDevice;
    *ppDataStream = pDataStream;
    return returncode;
}


// ---------------------------------------------------------------------------------------------------------------------
int exitTest() {
    int returncode = 0;
    std::cout << "RELEASE" << std::endl;
    std::cout << "#######" << std::endl << std::endl;

    // Release buffers
    std::cout << "5.1.13   Releasing the resources " << std::endl;
    try {
        if (pBufferList) {
            pBufferList->DiscardAllBuffers();
            while (pBufferList->size() > 0) {
                pBuffer = *(pBufferList->begin());
                pBufferList->RevokeBuffer(pBuffer);
                delete pBuffer;
            }
            std::cout << "         buffers after revoke:    " << pBufferList->size() << std::endl;
        }
        if (pDataStream)
            pDataStream->Close();
        if (pDevice)
            pDevice->Close();
        if (pInterface)
            pInterface->Close();
        if (pSystem)
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
    return returncode;
}


//------------------------------------------------------------------------------
BGAPI2::Node* nodeEntry(BGAPI2::NodeMap* const pNodeMap, const char* const pName, const bool bRequired, unsigned int* const pMissing) {
    if (bRequired == false) {
        if (pNodeMap->GetNodePresent(pName) == false) {
            if (pMissing != NULL) {
                *pMissing += 1;
            }
            return NULL;
        }
    }

    return pNodeMap->GetNode(pName);
};
