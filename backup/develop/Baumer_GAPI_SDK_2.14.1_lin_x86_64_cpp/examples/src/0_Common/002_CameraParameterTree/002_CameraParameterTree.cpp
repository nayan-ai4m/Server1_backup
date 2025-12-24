/* Copyright 2019-2020 Baumer Optronic */
/*
    This example describes the FIRST STEPS of handling Baumer-GAPI SDK.
    The given source code applies to handling one system, one camera and display the list of features.
    Please see "Baumer-GAPI SDK Programmer's Guide" chapter 5.3
*/

#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <string>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

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

    BGAPI2::Node * pNode = NULL;
    BGAPI2::NodeMap * pNodeTree = NULL;

    void printNodeRecursive(BGAPI2::Node* pNode, int level);
    void printDeviceRemoteNodeInformation(BGAPI2::Device * pDevice, BGAPI2::String sNodeName);
    int returncode = 0;

    std::cout << std::endl;
    std::cout << "##########################################################" << std::endl;
    std::cout << "# PROGRAMMER'S GUIDE Example 002_CameraParameterTree.cpp #" << std::endl;
    std::cout << "##########################################################" << std::endl;
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
                std::cout << "  5.2.1   System Name:     "
                    << sysIterator->GetFileName() << std::endl;
                std::cout << "          System Type:     "
                    << sysIterator->GetTLType() << std::endl;
                std::cout << "          System Version:  "
                    << sysIterator->GetVersion() << std::endl;
                std::cout << "          System PathName: "
                    << sysIterator->GetPathName() << std::endl << std::endl;
                sSystemID = sysIterator->GetID();
                std::cout << "        Opened system - NodeList Information " << std::endl;
                std::cout << "          GenTL Version:   "
                    << sysIterator->GetNode("GenTLVersionMajor")->GetValue()
                    << "." << sysIterator->GetNode("GenTLVersionMinor")->GetValue() << std::endl << std::endl;


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
                                std::cout << "5.1.13   Close interface (" << deviceList->size()
                                    << " cameras found) " << std::endl << std::endl;
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
                std::cout << "5.1.7   Open first device " << std::endl;
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

                if (devIterator->GetTLType() == "GEV") {
                    // OPEN THE CAMERA WITH READ ONLY OPTION
                    devIterator->OpenReadOnly();
                } else if (devIterator->GetTLType() == "U3V") {
                    devIterator->Open();
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
                    std::cout << "          SerialNumber:           Not Available." << std::endl;
                }

                // DISPLAY DEVICEMANUFACTURERINFO
                if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceManufacturerInfo"))
                    std::cout << "          DeviceManufacturerInfo: "
                    << devIterator->GetRemoteNode("DeviceManufacturerInfo")->GetValue() << std::endl;

                // DISPLAY DEVICEFIRMWAREVERSION OR DEVICEVERSION
                if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceFirmwareVersion")) {
                    std::cout << "          DeviceFirmwareVersion:  "
                        << devIterator->GetRemoteNode("DeviceFirmwareVersion")->GetValue() << std::endl;
                } else if (devIterator->GetRemoteNodeList()->GetNodePresent("DeviceVersion")) {
                    std::cout << "          DeviceVersion:          "
                        << devIterator->GetRemoteNode("DeviceVersion")->GetValue() << std::endl;
                } else {
                    std::cout << "          DeviceVersion:          Not Available." << std::endl;
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


    // SHOW TREE OF DEVICE NODES
    std::cout << " " << std::endl;
    std::cout << "DEVICE NODE TREE" << std::endl;
    std::cout << "################" << std::endl << std::endl;
    std::cout << "5.3.1   NodeTree Count: "
        << pDevice->GetRemoteNodeTree()->GetNodeCount() << std::endl << std::endl;

    pNodeTree = pDevice->GetRemoteNodeTree();

    for (bo_uint64 i = 0; i < pDevice->GetRemoteNodeTree()->GetNodeCount(); i++) {
        pNode = pNodeTree->GetNodeByIndex(i);
        printNodeRecursive(pNode, 0);
    }


    // DEVICE REMOTE NODE INFORMATION
    std::cout << " " << std::endl;
    std::cout << "DEVICE REMOTE NODE INFORMATION" << std::endl;
    std::cout << "##############################" << std::endl;

    std::string sFeature = "TriggerSource";  // Example input for Node information
    std::cin.width(64);
    do {
        std::cout << std::endl;
        printDeviceRemoteNodeInformation(pDevice, sFeature.data());
        std::cout << "input name of the camera feature like 'ExposureTime' or 'exit' to stop: " << std::endl;
        std::cin >> sFeature;
    } while (sFeature.compare("exit"));

    std::cout << std::endl;


    std::cout << "RELEASE" << std::endl;
    std::cout << "#######" << std::endl << std::endl;

    // Release
    std::cout << "5.1.13   Releasing the resources " << std::endl;
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

    std::cout << "Input any number to close the program:";
    int endKey = 0;
    std::cin >> endKey;
    return returncode;
}


void printNodeRecursive(BGAPI2::Node* pNode, int level) {
    int white_spaces = level * 7 + 1;
    for (int i = 0; i < white_spaces; i++) std::cout << " ";

    if (pNode->GetInterface() == "ICategory") {
        std::cout << "[" << std::left << std::setw(12) << pNode->GetInterface() << std::right << "]";
        std::cout << " " << pNode->GetName() << std::endl;
        for (bo_uint64 j = 0; j < pNode->GetNodeTree()->GetNodeCount(); j++) {
            BGAPI2::Node * nSubNode = pNode->GetNodeTree()->GetNodeByIndex(j);
            printNodeRecursive(nSubNode, level + 1);
        }
    } else {
        try {
            std::cout << "[" << std::left << std::setw(12) << pNode->GetInterface() << std::right << "]";
            std::cout << " " << std::left << std::setw(44) << pNode->GetName() << std::right;
            if ((pNode->IsReadable()) && (pNode->GetVisibility() != "Invisible")) {
                if (pNode->GetInterface() == "IBoolean") {
                    std::cout << ": " << pNode->GetValue();
                }
                if (pNode->GetInterface() == "IEnumeration") {
                    std::cout << ": " << pNode->GetValue();
                }
                if (pNode->GetInterface() == "IFloat") {
                    std::cout << ": " << pNode->GetValue();
                }
                if (pNode->GetInterface() == "IInteger") {
                    std::cout << ": " << pNode->GetValue();
                }
                if (pNode->GetInterface() == "IString") {
                    std::cout << ": " << pNode->GetValue();
                }
            }
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }
    }
    std::cout << " " << std::endl;
    return;
}

void printDeviceRemoteNodeInformation(BGAPI2::Device * pDevice, BGAPI2::String sNodeName) {
    try {
        std::cout << "printDeviceRemoteNodeInformation: '" << sNodeName << "'" << std::endl;
        std::cout << " Node Interface:           "
            << pDevice->GetRemoteNode(sNodeName)->GetInterface() << std::endl;
        std::cout << " Node Name:                "
            << pDevice->GetRemoteNode(sNodeName)->GetName() << std::endl;
        std::cout << " Node Display Name:        "
            << pDevice->GetRemoteNode(sNodeName)->GetDisplayName() << std::endl;
        std::cout << " Node Description:         "
            << pDevice->GetRemoteNode(sNodeName)->GetDescription() << std::endl;
        std::cout << " Node Tool Tip:            "
            << pDevice->GetRemoteNode(sNodeName)->GetToolTip() << std::endl;
        std::cout << " Node Visibility:          "
            << pDevice->GetRemoteNode(sNodeName)->GetVisibility() << std::endl;
        std::cout << " Node Is Implemented:      "
            << pDevice->GetRemoteNode(sNodeName)->GetImplemented() << std::endl;
        std::cout << " Node Is Available:        "
            << pDevice->GetRemoteNode(sNodeName)->GetAvailable() << std::endl;
        std::cout << " Node Current Access Mode: "
            << pDevice->GetRemoteNode(sNodeName)->GetCurrentAccessMode() << std::endl;
        std::cout << " Node Is Selector:         "
            << pDevice->GetRemoteNode(sNodeName)->IsSelector() << std::endl;
        if ((pDevice->GetRemoteNode(sNodeName)->IsReadable() == true) &&
            (pDevice->GetRemoteNode(sNodeName)->GetVisibility() != "Invisible")) {
            if (pDevice->GetRemoteNode(sNodeName)->GetInterface() == "IBoolean") {
                std::cout << " Node Value:               "
                    << pDevice->GetRemoteNode(sNodeName)->GetValue() << std::endl;
            }
            if (pDevice->GetRemoteNode(sNodeName)->GetInterface() == "ICommand") {
                std::cout << " Node Is Done:             "
                    << pDevice->GetRemoteNode(sNodeName)->GetValue() << std::endl;
            }
            if (pDevice->GetRemoteNode(sNodeName)->GetInterface() == "IEnumeration") {
                std::cout << " Node Value:               "
                    << pDevice->GetRemoteNode(sNodeName)->GetValue() << std::endl;
                std::cout << " Node Value (Integer):     "
                    << pDevice->GetRemoteNode(sNodeName)->GetInt() << std::endl;
                std::cout << " Node Enumeration Count:   "
                    << pDevice->GetRemoteNode(sNodeName)->GetEnumNodeList()->GetNodeCount() << std::endl;
                for (bo_uint64 l = 0; l < pDevice->GetRemoteNode(sNodeName)->GetEnumNodeList()->GetNodeCount(); l++) {
                    BGAPI2::Node * nEnumNode = pDevice->GetRemoteNode(sNodeName)->GetEnumNodeList()->GetNodeByIndex(l);
                    if (nEnumNode->IsReadable() == true) {
                        std::cout << "                     ["
                            << std::setw(2) << l << "]: "
                            << pDevice->GetRemoteNode(sNodeName)->GetEnumNodeList()->GetNodeByIndex(l)->GetValue()
                            << std::endl;
                    }
                }
                std::cout << std::endl;
            }
            if (pDevice->GetRemoteNode(sNodeName)->GetInterface() == "IFloat") {
                std::cout << " Node Value:               "
                    << pDevice->GetRemoteNode(sNodeName)->GetValue() << std::endl;
                std::cout << " Node Double:              "
                    << pDevice->GetRemoteNode(sNodeName)->GetDouble() << std::endl;
                std::cout << " Node DoubleMin:           "
                    << pDevice->GetRemoteNode(sNodeName)->GetDoubleMin() << std::endl;
                std::cout << " Node DoubleMax:           "
                    << pDevice->GetRemoteNode(sNodeName)->GetDoubleMax() << std::endl;
                if (pDevice->GetRemoteNode(sNodeName)->HasUnit() == true) {
                    std::cout << " Node Unit:                "
                        << pDevice->GetRemoteNode(sNodeName)->GetUnit() << std::endl;
                }
            }
            if (pDevice->GetRemoteNode(sNodeName)->GetInterface() == "IInteger") {
                std::cout << " Node Value:               "
                    << pDevice->GetRemoteNode(sNodeName)->GetValue() << std::endl;
                std::cout << " Node Int:                 "
                    << pDevice->GetRemoteNode(sNodeName)->GetInt() << std::endl;
                std::cout << " Node IntMin:              "
                    << pDevice->GetRemoteNode(sNodeName)->GetIntMin() << std::endl;
                std::cout << " Node IntMax:              "
                    << pDevice->GetRemoteNode(sNodeName)->GetIntMax() << std::endl;
                std::cout << " Node IntInc:              "
                    << pDevice->GetRemoteNode(sNodeName)->GetIntInc() << std::endl;
                if (pDevice->GetRemoteNode(sNodeName)->HasUnit() == true) {
                    std::cout << " Node Unit:                "
                        << pDevice->GetRemoteNode(sNodeName)->GetUnit() << std::endl;
                }
            }
            if (pDevice->GetRemoteNode(sNodeName)->GetInterface() == "IString") {
                std::cout << " Node Value:               "
                    << pDevice->GetRemoteNode(sNodeName)->GetValue() << std::endl;
                std::cout << " Node String:              "
                    << pDevice->GetRemoteNode(sNodeName)->GetString() << std::endl;
                std::cout << " Node MaxStringLength:     "
                    << pDevice->GetRemoteNode(sNodeName)->GetMaxStringLength() << std::endl;
            }
            if (pDevice->GetRemoteNode(sNodeName)->GetInterface() == "IRegister") {
                std::cout << " Node Has Unit:            "
                    << pDevice->GetRemoteNode(sNodeName)->HasUnit() << std::endl;
            }
            if (pDevice->GetRemoteNode(sNodeName)->IsSelector() == true) {
                std::cout << " SelectedNodeList Count:   "
                    << pDevice->GetRemoteNode(sNodeName)->GetSelectedNodeList()->GetNodeCount() << std::endl;
                for (bo_uint64 l = 0;
                    l < pDevice->GetRemoteNode(sNodeName)->GetSelectedNodeList()->GetNodeCount();
                    l++) {
                    BGAPI2::Node* node = pDevice->GetRemoteNode(sNodeName)->GetSelectedNodeList()->GetNodeByIndex(l);
                    if ((node->IsReadable() == true) && (node->GetVisibility() != "Invisible")) {
                        std::cout << "                           "
                            << pDevice->GetRemoteNode(sNodeName)->GetSelectedNodeList()->GetNodeByIndex(l)->GetName()
                            << std::endl;
                    }
                }
                std::cout << std::endl;
            }
        }
        std::cout << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
    return;
}
