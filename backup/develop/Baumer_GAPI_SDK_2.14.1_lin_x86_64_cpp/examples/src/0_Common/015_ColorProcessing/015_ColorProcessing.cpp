/* Copyright 2019-2020 Baumer Optronic */
#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <string>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

BGAPI2::String cur_color_transformation_auto("Off");

BGAPI2::Node* GetFeature(BGAPI2::NodeMap* const pNodeMap, BGAPI2::String feature) {
    if (pNodeMap != NULL) {
        try {
            if (pNodeMap->GetNodePresent(feature)) {
                BGAPI2::Node* const pNode = pNodeMap->GetNode(feature);
                if ((pNode != NULL) && pNode->GetImplemented() && pNode->GetAvailable()) {
                    return pNode;
                }
            }
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }
    }
    return NULL;
}

// Readout device color matrix
bool GetDeviceColorMatrix(BGAPI2::Device* const pDevice, double dColorMatrix[9]) {
    bo_uint uMask = 0;
    try {
        BGAPI2::NodeMap* const pNodeMap = pDevice->GetRemoteNodeList();
        BGAPI2::Node* const pColorTransformationValue = GetFeature(pNodeMap, SFNC_COLORTRANSFORMATIONVALUE);
        BGAPI2::Node* const pColorTransformationValueSelector =
            GetFeature(pNodeMap, SFNC_COLORTRANSFORMATIONVALUESELECTOR);

        if ((pColorTransformationValueSelector) && (pColorTransformationValue)) {
            BGAPI2::NodeMap* const pColorTransformationNodeMap = pColorTransformationValueSelector->GetEnumNodeList();
            const bo_uint64 iSelectorCount = pColorTransformationNodeMap->GetNodeCount();
            const BGAPI2::String sOldSelector = pColorTransformationValueSelector->GetString();
            BGAPI2::String sSelector = sOldSelector;

            for (bo_uint64 iSelectorIndex = 0; iSelectorIndex < iSelectorCount; iSelectorIndex++) {
                BGAPI2::Node* const pNode = pColorTransformationNodeMap->GetNodeByIndex(iSelectorIndex);
                if (pNode->GetImplemented() && pNode->GetAvailable()) {
                    BGAPI2::String sGainItemValue = pNode->GetValue();

                    int iIndex = -1;
                    if (sGainItemValue == "Gain00") {
                        iIndex = 0;
                    } else if (sGainItemValue == "Gain01") {
                        iIndex = 1;
                    } else if (sGainItemValue == "Gain02") {
                        iIndex = 2;
                    } else if (sGainItemValue == "Gain10") {
                        iIndex = 3;
                    } else if (sGainItemValue == "Gain11") {
                        iIndex = 4;
                    } else if (sGainItemValue == "Gain12") {
                        iIndex = 5;
                    } else if (sGainItemValue == "Gain20") {
                        iIndex = 6;
                    } else if (sGainItemValue == "Gain21") {
                        iIndex = 7;
                    } else if (sGainItemValue == "Gain22") {
                        iIndex = 8;
                    }

                    if (iIndex >= 0) {
                        sSelector = sGainItemValue;
                        pColorTransformationValueSelector->SetString(sSelector);
                        dColorMatrix[iIndex] = pColorTransformationValue->GetDouble();
                        uMask |= 1 << iIndex;
                    }
                }
            }

            if (sSelector != sOldSelector) {
                pColorTransformationValueSelector->SetString(sOldSelector);
            }
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
    return (uMask == 0x1FF);
}

bool SetImageProcessorColorMatrix(BGAPI2::ImageProcessor* const pImageProcessor, const double dColorMatrix[9]) {
    bo_uint uMask = 0;
    try {
        BGAPI2::NodeMap* const pNodeMap = pImageProcessor->GetNodeList();
        BGAPI2::Node* const pColorTransformationValueSelector = pNodeMap->GetNode("ColorTransformationValueSelector");
        BGAPI2::Node* const pColorTransformationValue = pNodeMap->GetNode("ColorTransformationValue");
        for (int iIndex = 0; iIndex < 9; iIndex++) {
            pColorTransformationValueSelector->SetInt(iIndex);
            pColorTransformationValue->SetDouble(dColorMatrix[iIndex]);
            uMask |= (1 << iIndex);
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
    return (uMask == 0x1FF);
}


int Test() {
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

    int returncode = 0;

    std::cout << std::endl;
    std::cout << "###########################" << std::endl;
    std::cout << "# 015_ColorProcessing.cpp #" << std::endl;
    std::cout << "###########################" << std::endl;
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
        return returncode;
    }


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
                                std::cout << "        Opened interface - NodeList Information" << std::endl;
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
        std::cout << " No interface found " << std::endl;
        std::cout << std::endl << "End" << std::endl;
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
            BGAPI2::Device* const pCurrentDevice = *devIterator;

            std::cout << "  5.2.3   Device DeviceID:        " << devIterator->GetID() << std::endl;
            std::cout << "          Device Model:           " << pCurrentDevice->GetModel() << std::endl;
            std::cout << "          Device SerialNumber:    " << pCurrentDevice->GetSerialNumber() << std::endl;
            std::cout << "          Device Vendor:          " << pCurrentDevice->GetVendor() << std::endl;
            std::cout << "          Device TLType:          " << pCurrentDevice->GetTLType() << std::endl;
            std::cout << "          Device AccessStatus:    " << pCurrentDevice->GetAccessStatus() << std::endl;
            std::cout << "          Device UserID:          " << pCurrentDevice->GetDisplayName() << std::endl
                << std::endl;
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
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
            BGAPI2::Device* const pCurrentDevice = *devIterator;
            try {
                std::cout << "5.1.7   Open first device " << std::endl;
                std::cout << "          Device DeviceID:        " << devIterator->GetID() << std::endl;
                std::cout << "          Device Model:           " << pCurrentDevice->GetModel() << std::endl;
                std::cout << "          Device SerialNumber:    " << pCurrentDevice->GetSerialNumber() << std::endl;
                std::cout << "          Device Vendor:          " << pCurrentDevice->GetVendor() << std::endl;
                std::cout << "          Device TLType:          " << pCurrentDevice->GetTLType() << std::endl;
                std::cout << "          Device AccessStatus:    " << pCurrentDevice->GetAccessStatus() << std::endl;
                std::cout << "          Device UserID:          " << pCurrentDevice->GetDisplayName() << std::endl
                    << std::endl;

                pCurrentDevice->Open();
                BGAPI2::NodeMap* const pDeviceRemoteNodeList = pCurrentDevice->GetRemoteNodeList();

                sDeviceID = devIterator->GetID();
                std::cout << "        Opened device - RemoteNodeList Information " << std::endl;
                std::cout << "          Device AccessStatus:    " << pCurrentDevice->GetAccessStatus() << std::endl;

                // SERIAL NUMBER
                if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_SERIALNUMBER)) {
                    std::cout << "          DeviceSerialNumber:     "
                        << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_SERIALNUMBER)->GetValue() << std::endl;
                } else if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_ID)) {
                    std::cout << "          DeviceID (SN):          "
                        << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_ID)->GetValue() << std::endl;
                } else {
                    std::cout << "          SerialNumber:           Not Available " << std::endl;
                }

                // DISPLAY DEVICEMANUFACTURERINFO
                if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_MANUFACTURERINFO)) {
                    std::cout << "          DeviceManufacturerInfo: "
                        << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_MANUFACTURERINFO)->GetValue() << std::endl;
                }


                // DISPLAY DEVICEFIRMWAREVERSION OR DEVICEVERSION
                if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_FIRMWAREVERSION)) {
                    std::cout << "          DeviceFirmwareVersion:  "
                        << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_FIRMWAREVERSION)->GetValue() << std::endl;
                } else if (pDeviceRemoteNodeList->GetNodePresent(SFNC_DEVICE_VERSION)) {
                    std::cout << "          DeviceVersion:          "
                        << pCurrentDevice->GetRemoteNode(SFNC_DEVICE_VERSION)->GetValue() << std::endl;
                } else {
                    std::cout << "          DeviceVersion:          Not Available " << std::endl;
                }

                if (pCurrentDevice->GetTLType() == "GEV") {
                    const bo_int64 iGevCurrentIpAddress =
                        pCurrentDevice->GetRemoteNode(SFNC_GEV_CURRENTIPADDRESS)->GetInt();
                    const bo_int64 iGevCurrentSubnetMask =
                        pCurrentDevice->GetRemoteNode(SFNC_GEV_CURRENTSUBNETMASK)->GetInt();
                    std::cout << "          GevCCP:                 "
                        << pCurrentDevice->GetRemoteNode(SFNC_GEV_CCP)->GetValue() << std::endl;
                    std::cout << "          GevCurrentIPAddress:    "
                        << ((iGevCurrentIpAddress & 0xff000000) >> 24) << "."
                        << ((iGevCurrentIpAddress & 0x00ff0000) >> 16) << "."
                        << ((iGevCurrentIpAddress & 0x0000ff00) >> 8) << "."
                        << (iGevCurrentIpAddress & 0x0000ff) << std::endl;
                    std::cout << "          GevCurrentSubnetMask:   "
                        << ((iGevCurrentSubnetMask & 0xff000000) >> 24) << "."
                        << ((iGevCurrentSubnetMask & 0x00ff0000) >> 16) << "."
                        << ((iGevCurrentSubnetMask & 0x0000ff00) >> 8) << "."
                        << (iGevCurrentSubnetMask & 0x0000ff) << std::endl;
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
        std::cout << " No Device found " << std::endl;
        std::cout << std::endl << "End" << std::endl;
        pInterface->Close();
        pSystem->Close();
        BGAPI2::SystemList::ReleaseInstance();
        return returncode;
    } else {
        pDevice = (*deviceList)[sDeviceID];
    }

    BGAPI2::NodeMap* pDeviceRemoteMap = NULL;

    std::cout << "DEVICE PARAMETER SETUP" << std::endl;
    std::cout << "######################" << std::endl << std::endl;

    try {
        pDeviceRemoteMap = pDevice->GetRemoteNodeList();

        // SET TRIGGER MODE OFF (FreeRun)
        pDevice->GetRemoteNode(SFNC_TRIGGERMODE)->SetString("Off");
        std::cout << "         TriggerMode:             "
            << pDevice->GetRemoteNode(SFNC_TRIGGERMODE)->GetValue() << std::endl;
        std::cout << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }


    BGAPI2::Node* const pColorTransformationAuto = GetFeature(pDeviceRemoteMap, "ColorTransformationAuto");
    if (pColorTransformationAuto) {
        if (pColorTransformationAuto->IsReadable() && pColorTransformationAuto->IsWriteable()) {
            cur_color_transformation_auto = pColorTransformationAuto->GetValue();
            BGAPI2::Node* pOffNode = GetFeature(pColorTransformationAuto->GetEnumNodeList(), "Off");
            if (pOffNode) {
                pColorTransformationAuto->SetValue(pOffNode->GetValue());
            }
        }
    }
    BGAPI2::Node* const pColorTransformationFactoryListSelector =
        GetFeature(pDeviceRemoteMap, "ColorTransformationFactoryListSelector");
    if (pColorTransformationFactoryListSelector) {
        BGAPI2::NodeMap* pColorTransformationMap = pColorTransformationFactoryListSelector->GetEnumNodeList();
        bo_uint64 iMatrixCount = pColorTransformationMap->GetNodeCount();

        std::cout << "DEVICE FACTORY COLOR MATRICES" << std::endl;
        std::cout << "#############################" << std::endl << std::endl;

        for (bo_uint64 iMatrixIndex = 0; iMatrixIndex < iMatrixCount; iMatrixIndex++) {
            BGAPI2::Node* pColorMatrix = pColorTransformationMap->GetNodeByIndex(iMatrixIndex);
            if (pColorMatrix->GetAvailable() && pColorMatrix->GetImplemented()) {
                std::cout << (iMatrixIndex + 1) << " - " << pColorMatrix->GetValue() << std::endl;
            }
        }
        std::cout << std::endl;

        BGAPI2::Node* const pColorTransformationResetToFactoryList =
            GetFeature(pDeviceRemoteMap, "ColorTransformationResetToFactoryList");
        if (pColorTransformationResetToFactoryList) {
            std::cout << "Input the color matrix number to select: ";
            bo_uint64 iMatrixIndex = 0;
            std::cin >> iMatrixIndex;
            std::cout << std::endl;

            iMatrixIndex--;

            if ((iMatrixIndex >= 0) && (iMatrixIndex < iMatrixCount)) {
                pColorTransformationFactoryListSelector->SetString(
                    pColorTransformationMap->GetNodeByIndex(iMatrixIndex)->GetValue());
                pColorTransformationResetToFactoryList->Execute();
            }
        }
    }

    double dColorMatrix[9];
    BGAPI2::String sPixelFormatSrc = "";
    BGAPI2::String sPixelFormatDst = "";
    BGAPI2::Node* pDevicePixelFormat = NULL;
    BGAPI2::ImageProcessor* pImageProcessor = NULL;

    if (GetDeviceColorMatrix(pDevice, dColorMatrix)) {
        std::cout << "DEVICE COLOR MATRIX" << std::endl;
        std::cout << "###################" << std::endl << std::endl;

        const std::streamsize precision = std::cout.precision(3);
        const std::ios_base::fmtflags flags = std::cout.flags();
        std::cout.setf(std::ios_base::fixed | std::ios_base::right);
        std::cout << std::setw(6) << dColorMatrix[0] << " " << std::setw(6) << dColorMatrix[1] << " "
            << std::setw(6) << dColorMatrix[2] << std::endl;
        std::cout << std::setw(6) << dColorMatrix[3] << " " << std::setw(6) << dColorMatrix[4] << " "
            << std::setw(6) << dColorMatrix[5] << std::endl;
        std::cout << std::setw(6) << dColorMatrix[6] << " " << std::setw(6) << dColorMatrix[7] << " "
            << std::setw(6) << dColorMatrix[8] << std::endl;
        std::cout.precision(precision);
        std::cout.flags(flags);
        std::cout << std::endl;


        std::cout << "SUPPORTED DEVICE PIXEL FORMAT" << std::endl;
        std::cout << "#############################" << std::endl << std::endl;
        try {
            pDevicePixelFormat = pDevice->GetRemoteNode(SFNC_PIXELFORMAT);
            BGAPI2::NodeMap* const pNodeMap = pDevicePixelFormat->GetEnumNodeList();
            bo_uint64 count = pNodeMap->GetNodeCount();
            for (bo_uint64 i = 0; i < count; i++) {
                BGAPI2::Node* pNode = pNodeMap->GetNodeByIndex(i);
                if ((pNode->GetImplemented()) && (pNode->GetAvailable())) {
                    BGAPI2::String sPixelFormat = pNode->GetValue();
                    std::cout << " " << sPixelFormat << std::endl;
                    if (sPixelFormatSrc == "") {
                        std::string sString = sPixelFormat.get();
                        if (sString.find("Bayer") != std::string::npos) {
                            sPixelFormatSrc = sPixelFormat;
                            sPixelFormatDst = "BGR8";
                        }
                    }
                }
            }
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }
        std::cout << std::endl;
    }

    if ((sPixelFormatSrc == "") || (sPixelFormatDst == "")) {
        std::cout << "NO COLOR MATRIX SUPPORT" << std::endl << std::endl;
    } else {
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

        pImageProcessor = new BGAPI2::ImageProcessor();
        if (pImageProcessor != NULL) {
            // SET COLOR MATRIX TO IMAGE PROCESSOR
            std::cout << "         Set color matrix to image processor" << std::endl;
            SetImageProcessorColorMatrix(pImageProcessor, dColorMatrix);
        }

        const bo_uint64 uGetFilledBufferTimeout = 1000;

        // SEARCH FOR 'AcquisitionAbort'
        BGAPI2::Node* pDeviceAcquisitionAbort = NULL;
        if (pDevice->GetRemoteNodeList()->GetNodePresent(SFNC_ACQUISITION_ABORT)) {
            pDeviceAcquisitionAbort = pDevice->GetRemoteNode(SFNC_ACQUISITION_ABORT);
        }

        BGAPI2::Node* const pDeviceAcquisitionStart = pDevice->GetRemoteNode(SFNC_ACQUISITION_START);
        BGAPI2::Node* const pDeviceAcquisitionStop = pDevice->GetRemoteNode(SFNC_ACQUISITION_STOP);

        // ENSURE CAMERA IS STOPPED TO SET PIXEL FORMAT
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
        std::cout << std::endl;

        std::cout << "CAMERA START" << std::endl;
        std::cout << "############" << std::endl << std::endl;

        // SET PIXEL FORMAT
        std::cout << "         Set Pixel Format to " << sPixelFormatSrc << std::endl;
        pDevicePixelFormat->SetString(sPixelFormatSrc);
        std::cout << std::endl;

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
        std::cout << std::endl;

        // START CAMERA
        try {
            std::cout << "5.1.12   " << pDevice->GetModel() << " started " << std::endl << std::endl;
            pDeviceAcquisitionStart->Execute();
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }

        // CAPTURE 5 IMAGES
        const int uCaptureCount = 5;
        std::cout << " " << std::endl;
        std::cout << "CAPTURE " << uCaptureCount << " IMAGES" << std::endl;
        std::cout << "################" << std::endl << std::endl;

        try {
            BGAPI2::Image* pImageSrc = NULL;
            bo_uchar* pMemDst = NULL;
            bo_uint64 memSizeDst = 0;

            for (int i = 0; i < uCaptureCount; i++) {
                // WAIT FOR IMAGE
                BGAPI2::Buffer* pBufferFilled = pDataStream->GetFilledBuffer(uGetFilledBufferTimeout);  // timeout msec
                if (pBufferFilled == NULL) {
                    std::cout << "Error: Buffer Timeout after " << uGetFilledBufferTimeout << "msec" << std::endl;
                } else {
                    if (pBufferFilled->GetIsIncomplete() == true) {
                        std::cout << "Error: Image is incomplete" << std::endl;
                    } else {
                        const bo_uint w = static_cast<bo_uint>(pBufferFilled->GetWidth());
                        const bo_uint h = static_cast<bo_uint>(pBufferFilled->GetHeight());
                        void* const pMemSrc = pBufferFilled->GetMemPtr();
                        BGAPI2::String sBufferPixelFormat = pBufferFilled->GetPixelFormat();
                        const bo_uint64 memSizeSrc = pBufferFilled->GetMemSize();

                        std::cout << " Image " << std::setw(5) << pBufferFilled->GetFrameID()
                            << " received in memory address " << std::hex << pMemSrc << std::dec
                            << " [" << sBufferPixelFormat << "]" << std::endl;

                        if (pImageSrc == NULL) {
                            pImageSrc = pImageProcessor->CreateImage(w, h, sBufferPixelFormat, pMemSrc, memSizeSrc);
                        } else {
                            pImageSrc->Init(w, h, sBufferPixelFormat, pMemSrc, memSizeSrc);
                        }

                        if (pImageSrc != NULL) {
                            bo_uint64 bufferLength = pImageSrc->GetTransformBufferLength(sPixelFormatDst);

                            if (bufferLength > memSizeDst) {
                                if (pMemDst != NULL) {
                                    delete[] pMemDst;
                                    pMemDst = NULL;
                                    memSizeDst = 0;
                                }

                                pMemDst = new bo_uchar[static_cast<size_t>(bufferLength)];
                                memSizeDst = bufferLength;
                            }

                            std::cout << " convert to " << sPixelFormatDst << std::endl;
                            pImageProcessor->TransformImageToBuffer(pImageSrc, sPixelFormatDst, pMemDst, bufferLength);
                        }
                    }

                    // queue buffer again
                    pBufferFilled->QueueBuffer();
                }
            }

            if (pImageSrc != NULL) {
                pImageSrc->Release();
            }

            if (pMemDst != NULL) {
                delete[] pMemDst;
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

        // STOP DataStream acquisition
        try {
            // DataStream Statistic
            BGAPI2::NodeMap* const pNodeList = pDataStream->GetNodeList();
            std::cout << "         DataStream Statistics " << std::endl;
            std::cout << "           GoodFrames:            "
                << pNodeList->GetNode("GoodFrames")->GetInt() << std::endl;
            std::cout << "           CorruptedFrames:       "
                << pNodeList->GetNode("CorruptedFrames")->GetInt() << std::endl;
            std::cout << "           LostFrames:            "
                << pNodeList->GetNode("LostFrames")->GetInt() << std::endl;
            if (pDataStream->GetTLType() == "GEV") {
                std::cout << "           ResendRequests:        "
                    << pNodeList->GetNode("ResendRequests")->GetInt() << std::endl;
                std::cout << "           ResendPackets:         "
                    << pNodeList->GetNode("ResendPackets")->GetInt() << std::endl;
                std::cout << "           LostPackets:           "
                    << pNodeList->GetNode("LostPackets")->GetInt() << std::endl;
                std::cout << "           Bandwidth:             "
                    << pNodeList->GetNode("Bandwidth")->GetInt() << std::endl;
            }
            std::cout << std::endl;

            pDataStream->StopAcquisition();
            std::cout << "5.1.12   DataStream stopped " << std::endl;

            bufferList->FlushAllToInputQueue();
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }
        std::cout << std::endl;
    }


    std::cout << "RELEASE" << std::endl;
    std::cout << "#######" << std::endl << std::endl;

    if (bufferList) {
        // Release buffers
        std::cout << "5.1.13   Releasing the resources " << std::endl;
        try {
            bufferList->DiscardAllBuffers();
            while (bufferList->size() > 0) {
                pBuffer = *(bufferList->begin());
                bufferList->RevokeBuffer(pBuffer);
                delete pBuffer;
            }
            std::cout << "         buffers after revoke:    " << bufferList->size() << std::endl;
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }
    }

    if (pImageProcessor) {
        delete pImageProcessor;
    }

    if (pDataStream) {
        try {
            pDataStream->Close();
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }
    }

    // restore color transformation setting
    if (pColorTransformationAuto) {
        if (pColorTransformationAuto->IsReadable() && pColorTransformationAuto->IsWriteable()) {
            pColorTransformationAuto->SetValue(cur_color_transformation_auto);
        }
    }

    try {
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
    return returncode;
}

int main() {
    try {
        Test();
    }
    catch (BGAPI2::Exceptions::IException& e) {
        std::cout << "unhandled BGAPI exception \"" << e.GetType() << "\" " << e.GetFunctionName() << " "
            << e.GetErrorDescription() << "\n";
    }
    catch (const std::exception& e) {
        std::cout << "unhandled exception: \"" << e.what() << "\"\n";
    }
    catch (...) {
        std::cout << "unhandled exception\n";
    }

    std::cout << "Input any number to close the program:";
    int endKey = 0;
    std::cin >> endKey;
    return 0;
}
