/* Copyright 2019-2020 Baumer Optronic */
/*
    This example describes the FIRST STEPS of handling Baumer-GAPI SDK.
    The given source code applies to handling one system, one camera and eight images.
    Please see "Baumer-GAPI SDK Programmer's Guide" chapter 5.7 Chunk-Mode
*/

#include <stdio.h>
#include <iostream>
#include <iomanip>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

// Start pattern of BGAPI Image Header V2 of TXG cameras
#define BGAPI2_IMAGEHEADER_STARTPATTERN_V2        0x1A2A3A4A

// BGAPI Image Header V2 of TXG cameras (some features are supported by customized cameras only)
struct FreeformatHeader2{
    unsigned int uiStartPattern;           // 00 Start pattern of BGAPI Image Header V2
    unsigned int uiIdentifier;             // 01 hardware type id of camera
    unsigned int uiMaster0;                // 02 first master register
    unsigned int uiMaster1;                // 03 second master register
    unsigned int uiFormat0;                // 04 first format register
    unsigned int uiFormat1;                // 05 second master register
    unsigned int uiFormatConfig;           // 06 format configuration, e.g. binning, partial, ...
    unsigned int uiBitMode;                // 07 pixel resolution
    unsigned int uiExposureValue;          // 08 exposure value
    unsigned int uiExposureControl;        // 09 exposure control
    unsigned int uiGain;                   // 10 analog gain
    unsigned int uiOffset;                 // 11 analog offset
    unsigned int uiDigitalIO;              // 12 digital io state
    unsigned int uiFlashDelay;             // 13 flash delay
    unsigned int uiTrigger;                // 14 trigger mode settings
    unsigned int uiTriggerDelay;           // 15 trigger delay in us
    unsigned int uiTestpattern;            // 16 test pattern settings
    unsigned int uiPixelGainRed;           // 17 pixel gain red
    unsigned int uiPixelGainBlue;          // 18 pixel gain blue
    unsigned int uiPixelGainGreenRed;      // 19 pixel gain green red
    unsigned int uiPixelGainGreenBlue;     // 20 pixel gain green blue
    unsigned short usFrameStartPositionX;  // 21_0 start position x of current frame
    unsigned short usFrameStartPositionY;  // 21_1 start position y of current frame
    unsigned short usFrameExtensionX;      // 22_0 extension in x direction of current frame
    unsigned short usFrameExtensionY;      // 22_1 extension in y direction of current frame
    unsigned int uiLineCounter;            // 23 linecounter
    unsigned int uiSubFrameLength;         // 24 sub frame length
    unsigned int uiFrameCounter;           // 25 framecounter
    unsigned int uiUserDefined;            // 26 user defined special register
    unsigned int uiTemperature;            // 27 Temperature in C
    unsigned int uiSequencer;              // 28 sequencer configuration
    unsigned int uiDataFormat;             // 29 dataformat Rawbayer, RGB, YUV
    unsigned int uiTimeStamp;              // 30 timestamp for image capture
    unsigned int uiNotDefined[32];         // 31-62 Not defined
    unsigned int uiStopPattern;            // 63 Stop pattern of BGAPI Image Header V2
};


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
    BGAPI2::String sBufferID;

    BGAPI2::Node * pNode = NULL;

    bool bIsAvaliableBaumerImageHeader3 = false;    // HXG(CID03xxxx), MXG, MXU, VLG, VLU
    bool bIsAvaliableFreeFormatHeader3 = false;     // customized cameras
    bool bIsAvaliableFreeformatHeader2 = false;     // TXG, EXG
    bool bIsAvailableChunkExtendedMode = false;     // LXG

    bo_uint64 img_offset = 0;
    int returncode = 0;

    std::cout << std::endl;
    std::cout << "############################################" << std::endl;
    std::cout << "# PROGRAMMER'S GUIDE Example 007_Chunk.cpp #" << std::endl;
    std::cout << "############################################" << std::endl;
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
                std::cout << "          GenTL Version:   "
                    << sysIterator->GetNode("GenTLVersionMajor")->GetValue() << "."
                    << sysIterator->GetNode("GenTLVersionMinor")->GetValue() << std::endl << std::endl;

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
                        std::cout << "  5.2.2   Interface ID:      "
                            << ifIterator->GetID() << std::endl;
                        std::cout << "          Interface Type:    "
                            << ifIterator->GetTLType() << std::endl;
                        std::cout << "          Interface Name:    "
                            << ifIterator->GetDisplayName() << std::endl << std::endl;
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
                                << ifIterator->GetDisplayName() << std::endl;
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
                                    std::cout << "          GevInterfaceSubnetIPAddress: "
                                        << ifIterator->GetNode("GevInterfaceSubnetIPAddress")->GetValue()
                                        << std::endl;
                                    std::cout << "          GevInterfaceSubnetMask:      "
                                        << ifIterator->GetNode("GevInterfaceSubnetMask")->GetValue()
                                        << std::endl;
                                }
                                if (ifIterator->GetTLType() == "U3V") {
                                    // std::cout << "          NodeListCount:     "
                                    // << ifIterator->GetNodeList()->GetNodeCount() << std::endl;
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
            std::cout << "  5.2.3   Device DeviceID:        "
                << devIterator->GetID() << std::endl;
            std::cout << "          Device Model:           "
                << devIterator->GetModel() << std::endl;
            std::cout << "          Device SerialNumber:    "
                << devIterator->GetSerialNumber() << std::endl;
            std::cout << "          Device Vendor:          "
                << devIterator->GetVendor() << std::endl;
            std::cout << "          Device TLType:          "
                << devIterator->GetTLType() << std::endl;
            std::cout << "          Device AccessStatus:    "
                << devIterator->GetAccessStatus() << std::endl;
            std::cout << "          Device UserID:          "
                << devIterator->GetDisplayName() << std::endl << std::endl;
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
                std::cout << "5.1.7   Open device " << std::endl;
                std::cout << "          Device DeviceID:        "
                    << devIterator->GetID() << std::endl;
                std::cout << "          Device Model:           "
                    << devIterator->GetModel() << std::endl;
                std::cout << "          Device SerialNumber:    "
                    << devIterator->GetSerialNumber() << std::endl;
                std::cout << "          Device Vendor:          "
                    << devIterator->GetVendor() << std::endl;
                std::cout << "          Device TLType:          "
                    << devIterator->GetTLType() << std::endl;
                std::cout << "          Device AccessStatus:    "
                    << devIterator->GetAccessStatus() << std::endl;
                std::cout << "          Device UserID:          "
                    << devIterator->GetDisplayName() << std::endl << std::endl;
                devIterator->Open();
                if (devIterator->GetRemoteNodeList()->GetNodePresent("ChunkSelector") == false) {
                    std::cout << "          Device did not support Chunk!        " << std::endl;
                    devIterator->Close();
                    continue;
                }
                sDeviceID = devIterator->GetID();
                std::cout << "        Opened device - RemoteNodeList Information " << std::endl;
                std::cout << "          Device AccessStatus:    "
                    << devIterator->GetAccessStatus() << std::endl;

                // SERIAL NUMBER
                if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceSerialNumber")) {
                    std::cout << "          DeviceSerialNumber:     "
                        << devIterator->GetRemoteNode("DeviceSerialNumber")->GetValue() << std::endl;
                } else if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceID")) {
                    std::cout << "          DeviceID (SN):          "
                        << devIterator->GetRemoteNode("DeviceID")->GetValue() << std::endl;
                } else {
                    std::cout << "          SerialNumber:           Not Available " << std::endl;
                }

                // DISPLAY DEVICEMANUFACTURERINFO
                if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceManufacturerInfo")) {
                    std::cout << "          DeviceManufacturerInfo: "
                        << devIterator->GetRemoteNode("DeviceManufacturerInfo")->GetValue() << std::endl;
                }

                // DISPLAY DEVICEFIRMWAREVERSION OR DEVICEVERSION
                if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceFirmwareVersion")) {
                    std::cout << "          DeviceFirmwareVersion:  "
                        << devIterator->GetRemoteNode("DeviceFirmwareVersion")->GetValue() << std::endl;
                } else if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceVersion")) {
                    std::cout << "          DeviceVersion:          "
                        << devIterator->GetRemoteNode("DeviceVersion")->GetValue() << std::endl;
                } else {
                    std::cout << "          DeviceVersion:          Not Available " << std::endl;
                }

                if (devIterator->GetTLType() == "GEV") {
                    std::cout << "          GevCCP:                 "
                        << devIterator->GetRemoteNode("GevCCP")->GetValue() << std::endl;
                    std::cout << "          GevCurrentIPAddress:    "
                        << devIterator->GetRemoteNode("GevCurrentIPAddress")->GetValue() << std::endl;
                    std::cout << "          GevCurrentSubnetMask:   "
                        << devIterator->GetRemoteNode("GevCurrentSubnetMask")->GetValue() << std::endl;
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


    std::cout << "DEVICE PARAMETER SETUP" << std::endl;
    std::cout << "######################" << std::endl << std::endl;

    try {
        // SET TRIGGER SOURCE "SOFTWARE"
        BGAPI2::String sTriggerSourceNodeName = "";
        BGAPI2::NodeMap * pEnumNodes = pDevice->GetRemoteNode("TriggerSource")->GetEnumNodeList();
        if (pEnumNodes->GetNodePresent("SoftwareTrigger")) {
            sTriggerSourceNodeName = "SoftwareTrigger";
        } else if (pEnumNodes->GetNodePresent("Software")) {
            sTriggerSourceNodeName = "Software";
        }
        pDevice->GetRemoteNode("TriggerSource")->SetString(sTriggerSourceNodeName);
        std::cout << "          set value to:           "
            << pDevice->GetRemoteNode("TriggerSource")->GetValue() << std::endl << std::endl;

        // SET TRIGGER MODE ON
        pDevice->GetRemoteNode("TriggerMode")->SetString("On");
        std::cout << "        TriggerMode:              "
            << pDevice->GetRemoteNode("TriggerMode")->GetValue() << std::endl << std::endl;


        // SET CHUNK
        std::cout << "5.7.1   Feature 'Chunk'" << std::endl << std::endl;

        if (pDevice->GetRemoteNode("ChunkSelector")->GetEnumNodeList()->GetNodeCount() > 2) {
            bIsAvailableChunkExtendedMode = true;
            // ACTIVATE CHUNK
            pDevice->GetRemoteNode("ChunkModeActive")->SetBool(true);
            std::cout << "        ChunkModeActive:          "
                << pDevice->GetRemoteNode("ChunkModeActive")->GetValue() << std::endl;

            bo_uint64 nodeCount = pDevice->GetRemoteNode("ChunkSelector")->GetEnumNodeList()->GetNodeCount();
            for (bo_uint64 i = 0; i < nodeCount; i++) {
                BGAPI2::Node *pEnum = pDevice->GetRemoteNode("ChunkSelector")->GetEnumNodeList()->GetNodeByIndex(i);
                if (pEnum->IsReadable() && pDevice->GetRemoteNode("ChunkSelector")->IsWriteable()) {
                    pDevice->GetRemoteNode("ChunkSelector")->SetString(pEnum->GetString());

                    std::cout << "        ChunkSelector:            "
                        << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                    if (pDevice->GetRemoteNode("ChunkEnable")->IsWriteable()) {
                        pDevice->GetRemoteNode("ChunkEnable")->SetBool(true);
                    }
                    std::cout << "         ChunkEnable:             "
                        << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl;
                }
            }
            std::cout << std::endl;
        } else {
            // CHUNK TYPE "BaumerImageHeader3" of HXG(CID03xxxx), MXG, MXU, VLG, VLU
            if (pDevice->GetRemoteNode("ChunkSelector")->GetEnumNodeList()->GetNodePresent("BaumerImageHeader3")) {
                bIsAvaliableBaumerImageHeader3 = true;
                // ACTIVATE CHUNK
                pDevice->GetRemoteNode("ChunkModeActive")->SetBool(true);
                std::cout << "        ChunkModeActive:          "
                    << pDevice->GetRemoteNode("ChunkModeActive")->GetValue() << std::endl;
                // ENABLE ChunkSelector "BaumerImageHeader3"
                pDevice->GetRemoteNode("ChunkSelector")->SetString("BaumerImageHeader3");
                std::cout << "        ChunkSelector:            "
                    << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                if (pDevice->GetRemoteNode("ChunkEnable")->IsWriteable()) {
                    pDevice->GetRemoteNode("ChunkEnable")->SetBool(true);
                }
                std::cout << "         ChunkEnable:             "
                    << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl;
                // ENABLE ChunkSelector "Image"
                pDevice->GetRemoteNode("ChunkSelector")->SetString("Image");
                std::cout << "        ChunkSelector:            "
                    << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                if (pDevice->GetRemoteNode("ChunkEnable")->IsWriteable()) {
                    pDevice->GetRemoteNode("ChunkEnable")->SetBool(true);
                }
                std::cout << "         ChunkEnable:             "
                    << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl << std::endl;
            // CHUNK TYPE "FreeFormatHeader3"
            } else if (
                pDevice->GetRemoteNode("ChunkSelector")->GetEnumNodeList()->GetNodePresent("FreeFormatHeader3")) {
                bIsAvaliableFreeFormatHeader3 = true;
                // ACTIVATE CHUNK
                pDevice->GetRemoteNode("ChunkModeActive")->SetBool(true);
                std::cout << "        ChunkModeActive:          "
                    << pDevice->GetRemoteNode("ChunkModeActive")->GetValue() << std::endl;
                // ENABLE ChunkSelector "FreeFormatHeader3"
                pDevice->GetRemoteNode("ChunkSelector")->SetString("FreeFormatHeader3");
                std::cout << "        ChunkSelector:            "
                    << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                if (pDevice->GetRemoteNode("ChunkEnable")->IsWriteable()) {
                    pDevice->GetRemoteNode("ChunkEnable")->SetBool(true);
                }
                std::cout << "         ChunkEnable:             "
                    << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl;
                // ENABLE ChunkSelector "Image"
                pDevice->GetRemoteNode("ChunkSelector")->SetString("Image");
                std::cout << "        ChunkSelector:            "
                    << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                if (pDevice->GetRemoteNode("ChunkEnable")->IsWriteable()) {
                    pDevice->GetRemoteNode("ChunkEnable")->SetBool(true);
                }
                std::cout << "         ChunkEnable:             "
                    << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl << std::endl;
            // CHUNK TYPE "FreeformatHeader3" of SXG
            } else if (
                pDevice->GetRemoteNode("ChunkSelector")->GetEnumNodeList()->GetNodePresent("FreeformatHeader3")) {
                bIsAvaliableFreeFormatHeader3 = true;
                // ACTIVATE CHUNK
                pDevice->GetRemoteNode("ChunkModeActive")->SetBool(true);
                std::cout << "        ChunkModeActive:          "
                    << pDevice->GetRemoteNode("ChunkModeActive")->GetValue() << std::endl;
                // ENABLE ChunkSelector "FreeformatHeader3"
                pDevice->GetRemoteNode("ChunkSelector")->SetString("FreeformatHeader3");
                std::cout << "        ChunkSelector:            "
                    << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                if (pDevice->GetRemoteNode("ChunkEnable")->IsWriteable()) {
                    pDevice->GetRemoteNode("ChunkEnable")->SetBool(true);
                }
                std::cout << "         ChunkEnable:             "
                    << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl;
                // ENABLE ChunkSelector "Image"
                pDevice->GetRemoteNode("ChunkSelector")->SetString("Image");
                std::cout << "        ChunkSelector:            "
                    << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                if (pDevice->GetRemoteNode("ChunkEnable")->IsWriteable()) {
                    pDevice->GetRemoteNode("ChunkEnable")->SetBool(true);
                }
                std::cout << "         ChunkEnable:             "
                    << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl << std::endl;
            // CHUNK TYPE "FreeformatHeader2" of TXG, EXG
            } else if (
                pDevice->GetRemoteNode("ChunkSelector")->GetEnumNodeList()->GetNodePresent("FreeformatHeader2")) {
                bIsAvaliableFreeformatHeader2 = true;
                // ACTIVATE CHUNK
                pDevice->GetRemoteNode("ChunkModeActive")->SetBool(true);
                std::cout << "        ChunkModeActive:          "
                    << pDevice->GetRemoteNode("ChunkModeActive")->GetValue() << std::endl;
                // ENABLE ChunkSelector "FreeformatHeader2"
                pDevice->GetRemoteNode("ChunkSelector")->SetString("FreeformatHeader2");
                std::cout << "        ChunkSelector:            "
                    << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                if (pDevice->GetRemoteNode("ChunkEnable")->IsWriteable()) {
                    pDevice->GetRemoteNode("ChunkEnable")->SetBool(true);
                }
                std::cout << "         ChunkEnable:             "
                    << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl;
                // ENABLE ChunkSelector "Image"
                pDevice->GetRemoteNode("ChunkSelector")->SetString("Image");
                std::cout << "        ChunkSelector:            "
                    << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                if (pDevice->GetRemoteNode("ChunkEnable")->IsWriteable()) {
                    pDevice->GetRemoteNode("ChunkEnable")->SetBool(true);
                }
                std::cout << "         ChunkEnable:             "
                    << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl << std::endl;
            // CHUNK TYPE unkonwn
            } else {
                std::cout << "        Chunk not used" << std::endl;
                pDevice->GetRemoteNode("ChunkModeActive")->SetBool(false);
                // pDevice->GetRemoteNode("ChunkEnable")->SetBool(false);
                std::cout << "        ChunkModeActive:          "
                    << pDevice->GetRemoteNode("ChunkModeActive")->GetValue() << std::endl;
                std::cout << "        ChunkSelector:            "
                    << pDevice->GetRemoteNode("ChunkSelector")->GetValue() << std::endl;
                std::cout << "         ChunkEnable:             "
                    << pDevice->GetRemoteNode("ChunkEnable")->GetValue() << std::endl;
                std::cout << " " << std::endl;
            }
        }
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
            std::cout << "          StreamAnnounceBufferMinimum:  "
                << pDataStream->GetNode("StreamAnnounceBufferMinimum")->GetValue() << std::endl;
            if (pDataStream->GetTLType() == "GEV") {
                std::cout << "          StreamDriverModel:            "
                    << pDataStream->GetNode("StreamDriverModel")->GetValue() << std::endl;
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

    try {
        // BufferList
        bufferList = pDataStream->GetBufferList();

        // 4 buffers using internal buffer mode
        for (int i = 0; i < 4; i++) {
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
    std::cout << " " << std::endl;

    std::cout << "CAMERA START" << std::endl;
    std::cout << "############" << std::endl << std::endl;

    // START DataStream acquisition
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
        pDevice->GetRemoteNode("AcquisitionStart")->Execute();
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    // CAPTURE 8 IMAGES
    std::cout << " " << std::endl;
    std::cout << "TRIGGER BY SOFTWARE & CAPTURE 8 IMAGES WITH CHUNK IN IMAGE POLLING MODE" << std::endl;
    std::cout << "#######################################################################" << std::endl << std::endl;

    // SET FRAMECOUNTER
    if (pDevice->GetTLType() == "GEV") {
        if (pDevice->GetRemoteNodeList()->GetNodePresent("FrameCounter")) {
            std::cout << " SetFramecounter " << 1 << std::endl << std::endl;
            pDevice->GetRemoteNode("FrameCounter")->SetInt(1);
        }
    }

    BGAPI2::Buffer * pBufferFilled = NULL;
    try {
        for (int i = 0; i < 8; i++) {
            // SOFTWARE TRIGGER
            std::cout << " Execute TriggerSoftware " << i + 1 << std::endl;
            pDevice->GetRemoteNode("TriggerSoftware")->Execute();

            // WAIT FOR IMAGE
            pBufferFilled = pDataStream->GetFilledBuffer(1000);  // timeout 1000 msec
            if (pBufferFilled == NULL) {
                std::cout << "Error: Buffer Timeout after 1000 msec" << std::endl;
            } else if (pBufferFilled->GetIsIncomplete() == true) {
                std::cout << "Error: Image is incomplete" << std::endl;
                // QUEUE BUFFER
                pBufferFilled->QueueBuffer();
            } else {
                std::cout << " Image " << std::setw(5) << pBufferFilled->GetFrameID() << " received in memory address "
                    << std::hex << pBufferFilled->GetMemPtr() << std::dec << std::endl;

                // IF CHUNK IS NOT USED
                if ((bIsAvaliableBaumerImageHeader3 == false) &&
                    (bIsAvaliableFreeFormatHeader3 == false) &&
                    (bIsAvaliableFreeformatHeader2 == false) &&
                    (bIsAvailableChunkExtendedMode == false)) {
                    std::cout << "   Buffer.PixelFormat:            " << pBufferFilled->GetPixelFormat() << std::endl;
                    std::cout << "   Buffer.Width:                  " << pBufferFilled->GetWidth() << std::endl;
                    std::cout << "   Buffer.Height:                 " << pBufferFilled->GetHeight() << std::endl;
                } else if ((bIsAvaliableBaumerImageHeader3 == true) ||
                    (bIsAvaliableFreeFormatHeader3 == true) ||
                    (bIsAvailableChunkExtendedMode == true)) {
                    std::cout << "   Buffer.PixelFormat:            " << pBufferFilled->GetPixelFormat() << std::endl;
                    std::cout << "   Buffer.Width:                  " << pBufferFilled->GetWidth() << std::endl;
                    std::cout << "   Buffer.Height:                 " << pBufferFilled->GetHeight() << std::endl;

                    img_offset = pBufferFilled->GetImageOffset();
                    std::cout << "   Buffer.ImageOffset:            " << img_offset << std::endl;

                    // CHECK THE NUMBER OF ITEMS IN THE CHUNK
                    std::cout << "   ChunkNodeTree.Count:           "
                        << pBufferFilled->GetChunkNodeList()->GetNodeCount() << std::endl;
                    for (bo_uint64 j = 0; j < pBufferFilled->GetChunkNodeList()->GetNodeCount(); j++) {
                        pNode = pBufferFilled->GetChunkNodeList()->GetNodeByIndex(j);
                        // CHECK EACH ITEM IS ACCESSIBLE
                        if (pNode->IsReadable()) {
                            // PRINT ITEM NAME AND VALUE DEPENDING ON THE INTERFACE TYPE
                            if (pNode->GetInterface() == "IBoolean") {
                                std::cout << "        ["
                                    << std::left << std::setw(12) << pNode->GetInterface() << std::right << "]";
                                std::cout << " " << std::left << std::setw(33) << pNode->GetName() << std::right;
                                std::cout << ": " << pNode->GetBool() << std::endl;
                            }
                            if (pNode->GetInterface() == "IInteger") {
                                std::cout << "        ["
                                    << std::left << std::setw(12) << pNode->GetInterface() << std::right << "]";
                                std::cout << " " << std::left << std::setw(33) << pNode->GetName() << std::right;
                                std::cout << ": " << pNode->GetInt() << std::endl;
                            }
                            if (pNode->GetInterface() == "IEnumeration") {
                                // IF ITEM IS SELECTOR THEN SWITCH THROUGH ALL POSSIBLE SELECTIONS AND GET THEIR VALUES
                                if (pNode->IsSelector() == true) {
                                    // SAVE ORIGINAL SETTING OF SELECTOR
                                    BGAPI2::String sSavedSelector = pNode->GetString();

                                    // GET A LIST OF ITEMS THAT ARE RELATED TO THIS SELECTOR
                                    BGAPI2::NodeMap * nSelectedNodeList = pNode->GetSelectedNodeList();

                                    bool IsFirstAvailableNodeInList = true;
                                    for (bo_uint64 l = 0; l < pNode->GetEnumNodeList()->GetNodeCount(); l++) {
                                        BGAPI2::Node * nEnumNode = pNode->GetEnumNodeList()->GetNodeByIndex(l);
                                        if ((nEnumNode->IsReadable() == true) &&
                                            (nEnumNode->GetVisibility() != "Invisible")) {
                                            if (IsFirstAvailableNodeInList == true) {
                                                std::cout << "        [" << std::left << std::setw(12)
                                                    << pNode->GetInterface() << std::right << "]";
                                                IsFirstAvailableNodeInList = false;
                                            } else {
                                                std::cout << "                      ";
                                            }
                                            std::cout << " " << std::left << std::setw(33) << pNode->GetName() << ": "
                                                << pNode->GetEnumNodeList()->GetNodeByIndex(l)->GetValue()
                                                << std::right << std::endl;
                                            // SET THE NEXT SELECTION
                                            pNode->SetString(pNode->GetEnumNodeList()->GetNodeByIndex(l)->GetValue());
                                            for (bo_uint64 s = 0; s < nSelectedNodeList->GetNodeCount(); s++) {
                                                BGAPI2::Node * nSelectedNode = nSelectedNodeList->GetNodeByIndex(s);
                                                if ((nSelectedNode->IsReadable() == true) &&
                                                    (nSelectedNode->GetVisibility() != "Invisible")) {
                                                    std::cout << "                        "
                                                        << std::left << std::setw(32) << nSelectedNode->GetName()
                                                        << std::right << ": " << nSelectedNode->GetValue()
                                                        << std::endl;
                                                }
                                            }
                                        }
                                    }
                                    // RESTORE ORIGINAL SETTING OF SELECTOR
                                    pNode->SetString(sSavedSelector);
                                } else {  // IF ITEM IS NOT A SELECTOR THEN OUTPUT VALUE
                                    std::cout << "        ["
                                        << std::left << std::setw(12) << pNode->GetInterface() << std::right << "]";
                                    std::cout << " " << std::left << std::setw(33) << pNode->GetName() << std::right;
                                    std::cout << ": " << pNode->GetString() << std::endl;
                                }
                            }
                            if (pNode->GetInterface() == "IFloat") {
                                std::cout << "        ["
                                    << std::left << std::setw(12) << pNode->GetInterface() << std::right << "]";
                                std::cout << " " << std::left << std::setw(33) << pNode->GetName() << std::right;
                                std::cout << ": " << pNode->GetDouble() << std::endl;
                            }
                            if (pNode->GetInterface() == "IString") {
                                std::cout << "        ["
                                    << std::left << std::setw(12) << pNode->GetInterface() << std::right << "]";
                                std::cout << " " << std::left << std::setw(33) << pNode->GetName() << std::right;
                                std::cout << ": " << pNode->GetString() << std::endl;
                            }
                        }
                    }
                // IF CHUNK IS "FreeformatHeader2"
                } else if (bIsAvaliableFreeformatHeader2 == true) {
                    std::cout << "   Buffer.PixelFormat:            " << pBufferFilled->GetPixelFormat() << std::endl;
                    std::cout << "   Buffer.Width:                  " << pBufferFilled->GetWidth() << std::endl;
                    std::cout << "   Buffer.Height:                 " << pBufferFilled->GetHeight() << std::endl;

                    img_offset = pBufferFilled->GetImageOffset();
                    std::cout << "   Buffer.ImageOffset:            " << img_offset << std::endl;

                    FreeformatHeader2 *pFreeformatHeader2 =
                        reinterpret_cast<FreeformatHeader2*>(pBufferFilled->GetMemPtr());
                    if (pFreeformatHeader2->uiStartPattern == BGAPI2_IMAGEHEADER_STARTPATTERN_V2) {
                        std::cout << "    Chunk.ExposureTime:           "
                            << pFreeformatHeader2->uiExposureValue << std::endl;
                        std::cout << "    Chunk.Gain:                   "
                            << pFreeformatHeader2->uiGain << std::endl;
                        std::cout << "    Chunk.BlackLevelOffset:       "
                            << pFreeformatHeader2->uiOffset << std::endl;
                        std::cout << "    Chunk.OffsetX:                "
                            << pFreeformatHeader2->usFrameStartPositionX << std::endl;
                        std::cout << "    Chunk.OffsetY:                "
                            << pFreeformatHeader2->usFrameStartPositionY << std::endl;
                        std::cout << "    Chunk.Width:                  "
                            << pFreeformatHeader2->usFrameExtensionX << std::endl;
                        std::cout << "    Chunk.Height:                 "
                            << pFreeformatHeader2->usFrameExtensionY << std::endl;
                        std::cout << "    Chunk.FrameCounter:           "
                            << pFreeformatHeader2->uiFrameCounter << std::endl;
                        std::cout << "    Chunk.PixelFormatHex:         0x"
                            << std::hex << pFreeformatHeader2->uiDataFormat << std::dec << std::endl;
                        BGAPI2::String sPixelFormat = "";
                        switch (pFreeformatHeader2->uiDataFormat) {
                            // TXG PixelFormats
                        case 0x01080001: sPixelFormat = "Mono8"; break;
                        case 0x01100003: sPixelFormat = "Mono10"; break;
                        case 0x010C0004: sPixelFormat = "Mono10Packed"; break;
                        case 0x01100005: sPixelFormat = "Mono12"; break;
                        case 0x010C0006: sPixelFormat = "Mono12Packed"; break;
                        case 0x0108000A: sPixelFormat = "BayerGB8"; break;
                        case 0x0110000E: sPixelFormat = "BayerGB10"; break;
                        case 0x01100012: sPixelFormat = "BayerGB12"; break;
                        case 0x01080009: sPixelFormat = "BayerRG8"; break;
                        case 0x0110000D: sPixelFormat = "BayerRG10"; break;
                        case 0x01100011: sPixelFormat = "BayerRG12"; break;
                        case 0x02180015: sPixelFormat = "BGR8Packed"; break;
                        case 0x02180014: sPixelFormat = "RGB8Packed"; break;
                        case 0x020C001E: sPixelFormat = "YUV411Packed"; break;
                        case 0x0210001F: sPixelFormat = "YUV422Packed"; break;
                        case 0x02180020: sPixelFormat = "YUV444Packed"; break;
                        default:         sPixelFormat = "unknown";
                        }
                        std::cout << "    Chunk.PixelFormat:            " << sPixelFormat << std::endl;
                    } else {
                        std::cout << "   FreeformatHeader2 is not available." << std::endl;
                    }
                }

                std::cout << std::endl;

                // QUEUE BUFFER
                pBufferFilled->QueueBuffer();
            }
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
    std::cout << " " << std::endl;


    std::cout << "CAMERA STOP" << std::endl;
    std::cout << "###########" << std::endl << std::endl;

    // STOP CAMERA
    try {
        if (pDevice->GetRemoteNodeList()->GetNodePresent("AcquisitionAbort")) {
            pDevice->GetRemoteNode("AcquisitionAbort")->Execute();
            std::cout << "5.1.12   " << pDevice->GetModel() << " aborted " << std::endl;
        }
        pDevice->GetRemoteNode("AcquisitionStop")->Execute();
        std::cout << "5.1.12   " << pDevice->GetModel() << " stopped " << std::endl;
        std::cout << std::endl;

        BGAPI2::String sExposureNodeName = "";
        if (pDevice->GetRemoteNodeList()->GetNodePresent("ExposureTime")) {
            sExposureNodeName = "ExposureTime";
        } else if (pDevice->GetRemoteNodeList()->GetNodePresent("ExposureTimeAbs")) {
            sExposureNodeName = "ExposureTimeAbs";
        }
        std::cout << "         ExposureTime:                   "
            << std::fixed << std::setprecision(0) << pDevice->GetRemoteNode(sExposureNodeName)->GetDouble() << " ["
            << pDevice->GetRemoteNode(sExposureNodeName)->GetUnit() << "]" << std::endl;
        if (pDevice->GetTLType() == "GEV") {
            if (pDevice->GetRemoteNodeList()->GetNodePresent("DeviceStreamChannelPacketSize")) {
                std::cout << "         DeviceStreamChannelPacketSize:  "
                    << pDevice->GetRemoteNode("DeviceStreamChannelPacketSize")->GetInt() << " [bytes]" << std::endl;
            } else {
                std::cout << "         GevSCPSPacketSize:              "
                    << pDevice->GetRemoteNode("GevSCPSPacketSize")->GetInt() << " [bytes]" << std::endl;
            }
            std::cout << "         GevSCPD (PacketDelay):          "
                << pDevice->GetRemoteNode("GevSCPD")->GetInt() << " [tics]" << std::endl;
        }
        std::cout << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    // STOP DataStream acquisition
    try {
        if (pDataStream->GetTLType() == "GEV") {
            // DataStream Statistic
            std::cout << "         DataStream Statistics " << std::endl;
            std::cout << "           DataBlockComplete:              "
                << pDataStream->GetNodeList()->GetNode("DataBlockComplete")->GetInt() << std::endl;
            std::cout << "           DataBlockInComplete:            "
                << pDataStream->GetNodeList()->GetNode("DataBlockInComplete")->GetInt() << std::endl;
            std::cout << "           DataBlockMissing:               "
                << pDataStream->GetNodeList()->GetNode("DataBlockMissing")->GetInt() << std::endl;
            std::cout << "           PacketResendRequestSingle:      "
                << pDataStream->GetNodeList()->GetNode("PacketResendRequestSingle")->GetInt() << std::endl;
            std::cout << "           PacketResendRequestRange:       "
                << pDataStream->GetNodeList()->GetNode("PacketResendRequestRange")->GetInt() << std::endl;
            std::cout << "           PacketResendReceive:            "
                << pDataStream->GetNodeList()->GetNode("PacketResendReceive")->GetInt() << std::endl;
            std::cout << "           DataBlockDroppedBufferUnderrun: "
                << pDataStream->GetNodeList()->GetNode("DataBlockDroppedBufferUnderrun")->GetInt() << std::endl;
            std::cout << "           Bitrate:                        "
                << pDataStream->GetNodeList()->GetNode("Bitrate")->GetDouble() << std::endl;
            std::cout << "           Throughput:                     "
                << pDataStream->GetNodeList()->GetNode("Throughput")->GetDouble() << std::endl;
            std::cout << std::endl;
        }
        if (pDataStream->GetTLType() == "U3V") {
            // DataStream Statistic
            std::cout << "         DataStream Statistics " << std::endl;
            std::cout << "           GoodFrames:            "
                << pDataStream->GetNodeList()->GetNode("GoodFrames")->GetInt() << std::endl;
            std::cout << "           CorruptedFrames:       "
                << pDataStream->GetNodeList()->GetNode("CorruptedFrames")->GetInt() << std::endl;
            std::cout << "           LostFrames:            "
                << pDataStream->GetNodeList()->GetNode("LostFrames")->GetInt() << std::endl;
            std::cout << std::endl;
        }

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
