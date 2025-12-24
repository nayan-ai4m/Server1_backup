/* Copyright 2019-2020 Baumer Optronic */
/*
    This example describes the FIRST STEPS of handling Baumer-GAPI SDK.
    The given source code applies to handling one system, one camera and sets the Heartbeat Timeout.
    Please see "Baumer-GAPI SDK Programmer's Guide" chapter 5.1 GEV Heartbeat Timeout during Debugging
*/

#include <stdio.h>
#include <iostream>
#include <iomanip>
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
    int returncode = 0;

    std::cout << std::endl;
    std::cout << "#######################################################" << std::endl;
    std::cout << "# PROGRAMMER'S GUIDE Example 101_HeartbeatTimeout.cpp #" << std::endl;
    std::cout << "#######################################################" << std::endl;
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
            std::cout << "  5.2.3   Device DeviceID:        " << devIterator->GetID() << std::endl;
            std::cout << "          Device Model:           " << devIterator->GetModel() << std::endl;
            std::cout << "          Device SerialNumber:    " << devIterator->GetSerialNumber() << std::endl;
            std::cout << "          Device Vendor:          " << devIterator->GetVendor() << std::endl;
            std::cout << "          Device TLType:          " << devIterator->GetTLType() << std::endl;
            std::cout << "          Device AccessStatus:    " << devIterator->GetAccessStatus() << std::endl;
            std::cout << "          Device UserID:          " << devIterator->GetDisplayName() << std::endl
                << std::endl;
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
                devIterator->Open();
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
        if (pDevice->GetTLType() == "GEV") {
            // SET GevHeartbeatTimeout
            // =======================
            std::cout << "Set HeartbeatTimeout to longer time than 3000 msec during debugging" << std::endl;
            std::cout << "to prevent the camera from auto-disconnection in breakpoints" << std::endl << std::endl;

            if (pDevice->GetRemoteNodeList()->GetNodePresent("DeviceLinkHeartbeatTimeout")) {
                // 1 minute = 60,000,000 usec
                pDevice->GetRemoteNode("DeviceLinkHeartbeatTimeout")->SetDouble(60000000.0);

                std::cout << "    DeviceLinkHeartbeatTimeout: "
                    << pDevice->GetRemoteNode("DeviceLinkHeartbeatTimeout")->GetValue();
                if (pDevice->GetRemoteNode("DeviceLinkHeartbeatTimeout")->HasUnit() == true) {
                    std::cout << " [" << pDevice->GetRemoteNode("DeviceLinkHeartbeatTimeout")->GetUnit() << "] "
                        << std::endl << std::endl;
                } else {
                    std::cout << " [micro seconds]" << std::endl;
                }
            } else {
                pDevice->GetRemoteNode("GevHeartbeatTimeout")->SetInt(60000);  // 1 minute = 60000 msec

                std::cout << "    GevHeartbeatTimeout: " << pDevice->GetRemoteNode("GevHeartbeatTimeout")->GetValue();
                if (pDevice->GetRemoteNode("GevHeartbeatTimeout")->HasUnit() == true) {
                    std::cout << " [" << pDevice->GetRemoteNode("GevHeartbeatTimeout")->GetUnit() << "] "
                        << std::endl << std::endl;
                } else {
                    std::cout << " [milli seconds]" << std::endl;
                }
            }
        } else if (pDevice->GetTLType() == "U3V") {
            std::cout << "USB3 does not support GevHeartbeatTimeout " << std::endl;
        }
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
