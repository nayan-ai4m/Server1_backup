/* Copyright 2019-2020 Baumer Optronic */
/*
    This example describes the FIRST STEPS of handling Baumer-GAPI SDK.
    The given source code applies to handling one system, one camera and ten images.
    Please see "Baumer-GAPI SDK Programmer's Guide" chapter 5.6
*/

#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <string>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

// GLOBAL VARIABLES
bo_double gfTimestampTickFrequency = 1.0;
bo_double gfMessageTimestampMicroSeconds_ExposureStart = 0.0;
bo_double gfMessageTimestampMicroSeconds_ExposureEnd = 0.0;
bo_double gfMessageTimestampMicroSeconds_ExposureDiff = 0.0;
bo_double gfMessageTimestampMicroSeconds_FrameTransferEnd = 0.0;

// CALLBACK FUNCTION DEFINITION
void BGAPI2CALL DevEventHandler(void * callbackOwner, BGAPI2::Events::DeviceEvent * pDevEvent);

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

    bool bIsAvaliableEventSelectorExposureStart = false;
    bool bIsAvaliableEventSelectorExposureEnd = false;
    bool bIsAvaliableEventSelectorFrameTransferEnd = false;
    int returncode = 0;

    std::cout << std::endl;
    std::cout << "##############################################################" << std::endl;
    std::cout << "# PROGRAMMER'S GUIDE Example 012_DeviceEventMode_Handler.cpp #" << std::endl;
    std::cout << "##############################################################" << std::endl;
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
                if (devIterator->GetRemoteNodeList()->GetNodePresent("EventSelector") == false) {
                    std::cout << "          Device did not support Events!        " << std::endl;
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

        std::cout << "5.6.1   Device Events" << std::endl << std::endl;

        // CHECK AVAIALABILITY OF EVENT "ExposureStart"
        bIsAvaliableEventSelectorExposureStart = false;
        if (pDevice->GetRemoteNode("EventSelector")->GetEnumNodeList()->GetNodePresent("ExposureStart")) {
            bIsAvaliableEventSelectorExposureStart = true;
        }

        // CHECK AVAIALABILITY OF EVENT "ExposureEnd"
        bIsAvaliableEventSelectorExposureEnd = false;
        if (pDevice->GetRemoteNode("EventSelector")->GetEnumNodeList()->GetNodePresent("ExposureEnd")) {
            bIsAvaliableEventSelectorExposureEnd = true;
        }

        // CHECK AVAIALABILITY OF EVENT "FrameTransferEnd"
        bIsAvaliableEventSelectorFrameTransferEnd = false;
        if (pDevice->GetRemoteNode("EventSelector")->GetEnumNodeList()->GetNodePresent("FrameTransferEnd")) {
            bIsAvaliableEventSelectorFrameTransferEnd = true;
        }


        if ((bIsAvaliableEventSelectorExposureStart == true) || (bIsAvaliableEventSelectorExposureEnd == true)) {
            if (bIsAvaliableEventSelectorExposureStart) {
                // MESSAGE DEVICE EVENT "ExposureStart"
                // =====================================

                pDevice->GetRemoteNode("EventSelector")->SetString("ExposureStart");
                std::cout << "        EventSelector:            "
                    << pDevice->GetRemoteNode("EventSelector")->GetValue() << std::endl;

                if ((pDevice->GetRemoteNode("EventNotification")->GetEnumNodeList()->GetNodePresent("On")) == true) {
                    pDevice->GetRemoteNode("EventNotification")->SetString("On");  // standard: MXG, VisiLine,...
                    std::cout << "        EventNotification:        "
                        << pDevice->GetRemoteNode("EventNotification")->GetValue() << std::endl << std::endl;
                } else {
                    pDevice->GetRemoteNode("EventNotification")->SetString("GigEVisionEvent");  // TXG, SXG
                    std::cout << "        EventNotification:        "
                        << pDevice->GetRemoteNode("EventNotification")->GetValue() << std::endl << std::endl;
                }
            }

            if (bIsAvaliableEventSelectorExposureEnd) {
                // MESSAGE DEVEICE EVENT "ExposureEnd"
                // ====================================

                pDevice->GetRemoteNode("EventSelector")->SetString("ExposureEnd");
                std::cout << "        EventSelector:            "
                    << pDevice->GetRemoteNode("EventSelector")->GetValue() << std::endl;

                if ((pDevice->GetRemoteNode("EventNotification")->GetEnumNodeList()->GetNodePresent("On")) == true) {
                    pDevice->GetRemoteNode("EventNotification")->SetString("On");  // standard: MXG, VisiLine,...
                    std::cout << "        EventNotification:        "
                        << pDevice->GetRemoteNode("EventNotification")->GetValue() << std::endl << std::endl;
                } else {
                    pDevice->GetRemoteNode("EventNotification")->SetString("GigEVisionEvent");  // TXG, SXG
                    std::cout << "        EventNotification:        "
                        << pDevice->GetRemoteNode("EventNotification")->GetValue() << std::endl << std::endl;
                }
            }
        } else if (bIsAvaliableEventSelectorFrameTransferEnd == true) {
            // MESSAGE DEVEICE EVENT "FrameTransferEnd"
            // ====================================
            pDevice->GetRemoteNode("EventSelector")->SetValue("FrameTransferEnd");
            std::cout << "        EventSelector:            "
                << pDevice->GetRemoteNode("EventSelector")->GetValue() << std::endl;

            // enable event notification
            if ((pDevice->GetRemoteNode("EventNotification")->GetEnumNodeList()->GetNodePresent("On")) == true) {
                pDevice->GetRemoteNode("EventNotification")->SetValue("On");
                std::cout << "        EventNotification:        "
                    << pDevice->GetRemoteNode("EventNotification")->GetValue()
                    << std::endl;  // standard: MXG, VisiLine,...
            } else {
                pDevice->GetRemoteNode("EventNotification")->SetValue("GigEVisionEvent");  // TXG, SXG
                std::cout << "        EventNotification:        "
                    << pDevice->GetRemoteNode("EventNotification")->GetValue() << std::endl;
            }
            std::cout << std::endl;
        }

        // LIST UP DEVICE EVENT SETUP STATUS
        // ==================================

        // EVENT SELECTOR LIST
        std::cout << "        EventSelector list count: "
            << pDevice->GetRemoteNode("EventSelector")->GetEnumNodeList()->GetNodeCount() << std::endl;

        BGAPI2::String sEventSelectorItem;

        for (bo_uint64 i = 0; i < pDevice->GetRemoteNode("EventSelector")->GetEnumNodeList()->GetNodeCount(); i++) {
            pNode = pDevice->GetRemoteNode("EventSelector")->GetEnumNodeList()->GetNodeByIndex(i);
            if (pNode->IsReadable()) {
                std::cout << "                            " << std::setw(3) << i << ":  ";
                sEventSelectorItem = pNode->GetValue();
                std::cout << std::left << std::setw(28) << sEventSelectorItem << std::right;

                // check event notification is enabled ("On" or "GigeVisionEvent") or not ("Off")
                pDevice->GetRemoteNode("EventSelector")->SetValue(sEventSelectorItem);
                std::cout << " " << pDevice->GetRemoteNode("EventNotification")->GetValue() << std::endl;
            }
        }
        std::cout << std::endl;


        // REGISTER EVENT MODE EVENT_HANDLER
        // ==================================
        pDevice->RegisterDeviceEvent(BGAPI2::Events::EVENTMODE_EVENT_HANDLER);
        BGAPI2::Events::EventMode currentEventMode = pDevice->GetEventMode();
        BGAPI2::String sCurrentEventMode = "";
        switch (currentEventMode) {
        case BGAPI2::Events::EVENTMODE_POLLING:
            sCurrentEventMode = "EVENTMODE_POLLING";
            break;
        case BGAPI2::Events::EVENTMODE_UNREGISTERED:
            sCurrentEventMode = "EVENTMODE_UNREGISTERED";
            break;
        case BGAPI2::Events::EVENTMODE_EVENT_HANDLER:
            sCurrentEventMode = "EVENTMODE_EVENT_HANDLER";
            break;
        default:
            sCurrentEventMode = "EVENTMODE_UNKNOWN";
        }
        std::cout << "        Register Event Mode to:   " << sCurrentEventMode << std::endl << std::endl;

        // CALLBACK FUNCTION set
        std::cout << "REGISTER CALLBACK FUNCTION" << std::endl;
        std::cout << "##########################" << std::endl << std::endl;

        pDevice->RegisterDeviceEventHandler(pDevice, (BGAPI2::Events::DeviceEventHandler) &DevEventHandler);
        std::cout << std::endl;
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

    // CAPTURE 10 IMAGES
    std::cout << " " << std::endl;
    std::cout << "CAPTURE 10 IMAGES BY SOFTWARE TRIGGER" << std::endl;
    std::cout << "#####################################" << std::endl << std::endl;

    BGAPI2::Buffer * pBufferFilled = NULL;
    bo_double fExposureValue = 10000;
    bo_double fExposureTimeMin = 100.0;
    bo_double fExposureTimeMax = 1000.0;

    BGAPI2::String sExposureNodeName = "";
    if (pDevice->GetRemoteNodeList()->GetNodePresent("ExposureTime")) {
        sExposureNodeName = "ExposureTime";
    } else if (pDevice->GetRemoteNodeList()->GetNodePresent("ExposureTimeAbs")) {
        sExposureNodeName = "ExposureTimeAbs";
    }

    try {
        // The value of gfTimestampTickFrequency will be set before any read takes place - no lock is needed here
        if (pDevice->GetRemoteNodeList()->GetNodePresent("GevTimestampTickFrequency")) {
            gfTimestampTickFrequency = (bo_double)pDevice->GetRemoteNode("GevTimestampTickFrequency")->GetInt();
        } else {
            gfTimestampTickFrequency = 1000000000.0;
        }
        fExposureTimeMin = pDevice->GetRemoteNode(sExposureNodeName)->GetDoubleMin();
        fExposureTimeMax = pDevice->GetRemoteNode(sExposureNodeName)->GetDoubleMax();
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    try {
        for (int i = 0; i < 10; i++) {
            // INCREASE EXPOSURE TIME
            fExposureValue = fExposureValue + 10000.0;
            // check new value is within range
            if (fExposureValue < fExposureTimeMin)fExposureValue = fExposureTimeMin;
            if (fExposureValue > fExposureTimeMax) fExposureValue = fExposureTimeMax;
            pDevice->GetRemoteNode(sExposureNodeName)->SetDouble(fExposureValue);
            std::cout << " Set Exposure to: " << std::fixed << std::setprecision(0)
                << pDevice->GetRemoteNode(sExposureNodeName)->GetDouble() << " [usec]" << std::endl;

            // SOFTWARE TRIGGER
            std::cout << " Execute TriggerSoftware " << i + 1 << std::endl;
            pDevice->GetRemoteNode("TriggerSoftware")->Execute();


            // WAIT FOR IMAGE BUFFER IS FILLED
            pBufferFilled = pDataStream->GetFilledBuffer(1000);  // timeout 1000 msec
            if (pBufferFilled == NULL) {
                std::cout << "Error: Buffer Timeout after 1000 msec" << std::endl << std::endl;
            } else if (pBufferFilled->GetIsIncomplete() == true) {
                std::cout << "Error: Image is incomplete" << std::endl << std::endl;
                // queue buffer again
                pBufferFilled->QueueBuffer();
            } else {
                std::cout << " Image " << std::setw(5) << pBufferFilled->GetFrameID() << " received in memory address "
                    << std::hex << pBufferFilled->GetMemPtr() << std::dec << std::endl << std::endl;
                // queue buffer again
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


    // RESET EVENT MODE TO UNREGISTERED
    // =============================
    try {
        pDevice->UnregisterDeviceEvent();
        BGAPI2::Events::EventMode currentEventMode = pDevice->GetEventMode();
        BGAPI2::String sCurrentEventMode = "";
        switch (currentEventMode) {
        case BGAPI2::Events::EVENTMODE_POLLING:
            sCurrentEventMode = "EVENTMODE_POLLING";
            break;
        case BGAPI2::Events::EVENTMODE_UNREGISTERED:
            sCurrentEventMode = "EVENTMODE_UNREGISTERED";
            break;
        case BGAPI2::Events::EVENTMODE_EVENT_HANDLER:
            sCurrentEventMode = "EVENTMODE_EVENT_HANDLER";
            break;
        default:
            sCurrentEventMode = "EVENTMODE_UNKNOWN";
        }
        std::cout << "         Unregister Event Mode:   " << sCurrentEventMode << std::endl;
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
        // SEARCH FOR 'AcquisitionAbort'
        if (pDevice->GetRemoteNodeList()->GetNodePresent("AcquisitionAbort")) {
            pDevice->GetRemoteNode("AcquisitionAbort")->Execute();
            std::cout << "5.1.12   " << pDevice->GetModel() << " aborted " << std::endl;
        }

        pDevice->GetRemoteNode("AcquisitionStop")->Execute();
        std::cout << "5.1.12   " << pDevice->GetModel() << " stopped " << std::endl;
        std::cout << std::endl;

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

// CALLBACK
void BGAPI2CALL DevEventHandler(void * callbackOwner, BGAPI2::Events::DeviceEvent * pDevEvent) {
    if (NULL != pDevEvent) {
        std::cout << " [callback of " << ((BGAPI2::Device *)callbackOwner)->GetModel() << "] ";

        if (pDevEvent->GetName() == "ExposureStart") {
            try { gfMessageTimestampMicroSeconds_ExposureStart =
                (bo_double)pDevEvent->GetTimeStamp() / gfTimestampTickFrequency * 1000000.0; }
            catch (BGAPI2::Exceptions::IException&) { gfMessageTimestampMicroSeconds_ExposureStart = 0; }
            std::string sEventId = "", sEventName = "";
            try { sEventId = pDevEvent->GetId().get(); }
            catch (BGAPI2::Exceptions::IException&) { sEventId = ""; }
            try { sEventName = pDevEvent->GetName().get(); }
            catch (BGAPI2::Exceptions::IException&) { sEventName = ""; }
            std::cout << " EventID " << sEventId.c_str() << " " << std::setw(14) << sEventName.c_str() << " Timestamp "
                << std::fixed << std::setprecision(1) << gfMessageTimestampMicroSeconds_ExposureStart << " [usec]"
                << std::endl;
        } else if (pDevEvent->GetName() == "ExposureEnd") {
            try { gfMessageTimestampMicroSeconds_ExposureEnd =
                (bo_double)pDevEvent->GetTimeStamp() / gfTimestampTickFrequency * 1000000.0; }
            catch (BGAPI2::Exceptions::IException&) { gfMessageTimestampMicroSeconds_ExposureEnd = 0; }
            std::string sEventId = "", sEventName = "";
            try { sEventId = pDevEvent->GetId().get(); }
            catch (BGAPI2::Exceptions::IException&) { sEventId = ""; }
            try { sEventName = pDevEvent->GetName().get(); }
            catch (BGAPI2::Exceptions::IException&) { sEventName = ""; }
            std::cout << " EventID " << sEventId.c_str() << " " << std::setw(14) << sEventName.c_str() << " Timestamp "
                << std::fixed << std::setprecision(1) << gfMessageTimestampMicroSeconds_ExposureEnd << " [usec]"
                << std::endl;

            if (gfMessageTimestampMicroSeconds_ExposureStart != 0.0) {
                // CALCULATE TIME DIFFERENCE EXPOSURE END - START
                gfMessageTimestampMicroSeconds_ExposureDiff =
                    gfMessageTimestampMicroSeconds_ExposureEnd - gfMessageTimestampMicroSeconds_ExposureStart;
                std::cout << " Timestamp ExposureEnd - ExposureStart: "
                    << std::fixed << std::setprecision(1) << gfMessageTimestampMicroSeconds_ExposureDiff << " [usec]"
                    << std::endl;
            }
        } else if (pDevEvent->GetName() == "FrameTransferEnd") {
            bo_double fMessageTimestampMicroSecond = 0.0;
            try { fMessageTimestampMicroSecond =
                (bo_double)pDevEvent->GetTimeStamp() / gfTimestampTickFrequency * 1000000.0; }
            catch (BGAPI2::Exceptions::IException&) { fMessageTimestampMicroSecond = 0; }
            std::string sEventId = "", sEventName = "";
            try { sEventId = pDevEvent->GetId().get(); }
            catch (BGAPI2::Exceptions::IException&) { sEventId = ""; }
            try { sEventName = pDevEvent->GetName().get(); }
            catch (BGAPI2::Exceptions::IException&) { sEventName = ""; }
            std::cout << " EventID " << sEventId.c_str() << " " << std::setw(14) << sEventName.c_str() << " Timestamp "
                << std::fixed << std::setprecision(1) << fMessageTimestampMicroSecond << " [usec]" << std::endl;
        }
    } else {
        std::cout << " [callback] - received invalid Device Event! " << std::endl;
    }
}
