/* Copyright 2019-2020 Baumer Optronic */
/*
    This example describes the FIRST STEPS of handling Baumer-GAPI SDK.
    The given source code applies to handling one system, one camera and twelfe images.
    Please see "Baumer-GAPI SDK Programmer's Guide" chapter 5.1 and chapter 5.2
*/

#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

#if defined(_WIN32)
#include <windows.h>
#include <conio.h>
#else
#include <unistd.h>
#include <termios.h>
#endif

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

    bool bPerformUpdate = false;
    int iAppletIndex = -1;
    int returncode = 0;

    std::cout << std::endl;
    std::cout << "###################################################" << std::endl;
    std::cout << "# PROGRAMMER'S GUIDE Example 203_AppletUpdate.cpp #" << std::endl;
    std::cout << "###################################################" << std::endl;
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


    std::cout << "DEVICE ENABLE UPDATE MODE" << std::endl;
    std::cout << "#########################" << std::endl << std::endl;

    try {
        // CHECK IF UPDATE MODE IS AVAILABLE
        if (pDevice->IsUpdateModeAvailable()) {
            // SET DEVICE TO UPDATE MODE
            pDevice->SetUpdateMode(true, "");
        }
        std::cout << "    UpdateMode is: " << (pDevice->IsUpdateModeActive() ? "On" : "Off") << std::endl;

        // CHECK IF UPDATE MODE IS ACTIVE
        if (pDevice->IsUpdateModeActive()) {
            // DEVICE INFORMATION
            std::cout << "    Device DeviceID:                       "
                << pDevice->GetUpdateNode("DeviceID")->GetValue() << std::endl;
            std::cout << "    Device Model:                          "
                << pDevice->GetUpdateNode("DeviceModelName")->GetValue() << std::endl;
            std::cout << "    Device SerialNumber:                   "
                << pDevice->GetUpdateNode("DeviceSerialNumber")->GetValue() << std::endl;
            std::cout << "    Device Product ID:                     "
                << pDevice->GetUpdateNode("DeviceProductID")->GetValue() << std::endl;
            std::cout << "    Device Config ID (Firmware Version):   "
                << pDevice->GetUpdateNode("DeviceConfigID")->GetValue() << std::endl;

            // SET THE PATH TO THE UPDATE FILES
            std::cout << std::endl << "    Enter path to update files: ";
            std::string sUpdatePath;
            std::getline(std::cin, sUpdatePath);
            pDevice->GetUpdateNode("UpdatePath")->SetString(sUpdatePath.c_str());
            std::cout << "    Path to Update Files:                  "
                << pDevice->GetUpdateNode("UpdatePath")->GetValue() << std::endl;
            bo_int64 iAppletNumber = 0;
            if (pDevice->GetUpdateNode("AppletSelector")->GetAvailable()) {
                iAppletNumber = pDevice->GetUpdateNode("AppletSelector")->GetIntMax() + 1;
            }
            std::cout << "    Number of available Applets:           " << iAppletNumber << std::endl;
            for (int i = 0; i < iAppletNumber; i++) {
                pDevice->GetUpdateNode("AppletSelector")->SetInt(i);
                std::cout << "        AppletName:             [index " << std::setw(2) << i << "]:   "
                    << pDevice->GetUpdateNode("AppletName")->GetValue() << std::endl;
            }
            bPerformUpdate = iAppletNumber > 0;
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    if (bPerformUpdate) {
        std::cout << "  " << std::endl;
        std::cout << "PERFORM UPDATE" << std::endl;
        std::cout << "##############" << std::endl << std::endl;

        std::cout << "    Input a valid index to start the update or any other number to skip: ";
        std::string sAppletIndex;
        std::getline(std::cin, sAppletIndex);
        std::stringstream sInput(sAppletIndex);
        sInput >> iAppletIndex;
        std::cout << "  " << std::endl;
        try {
            bPerformUpdate = iAppletIndex <= pDevice->GetUpdateNode("AppletSelector")->GetIntMax();
            bPerformUpdate &= iAppletIndex >= pDevice->GetUpdateNode("AppletSelector")->GetIntMin();
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }
    }

    if (bPerformUpdate) {
        try {
            // CHECK IF UPDATE MODE IS ACTIVE
            if (pDevice->IsUpdateModeActive()) {
                if (pDevice->GetUpdateNode("AppletSelector")->GetAvailable()) {
                    pDevice->GetUpdateNode("AppletSelector")->SetInt(iAppletIndex);
                    std::cout << "    Selected Device Applet:    "
                        << pDevice->GetUpdateNode("AppletName")->GetValue() << std::endl;
                }
                std::cout << "    Start update!" << std::endl;
                pDevice->GetUpdateNode("StartAppletUpdate")->Execute();
                while (pDevice->GetUpdateNode("DeviceState")->GetValue() == "Updating") {
                    std::cout << "    Update Progress:  "
                        << std::setw(4) << pDevice->GetUpdateNode("UpdateProgress")->GetValue() << "%"
                        << '\r' << std::flush;
#if defined(_WIN32)
                    Sleep(1000);
#else
                    usleep(1000000);
#endif
                }
                std::cout << "    Update finished with result: "
                    << pDevice->GetUpdateNode("UpdateResult")->GetValue() << std::endl;
            }
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }


        std::cout << "  " << std::endl;
        std::cout << "PERFORM REBOOT" << std::endl;
        std::cout << "##############" << std::endl << std::endl;

        try {
            // CHECK IF UPDATE MODE IS ACTIVE
            if (pDevice->IsUpdateModeActive()) {
                if (pDevice->GetUpdateNode("StartReboot")->GetAvailable()) {
                    std::cout << "    Start reboot!" << std::endl;
                    pDevice->GetUpdateNode("StartReboot")->Execute();
                    std::cout << "    Waiting..";
                    while (pDevice->GetUpdateNode("DeviceState")->GetValue() == "Rebooting") {
                        std::cout << "." << std::flush;
#if defined(_WIN32)
                        Sleep(1000);
#else
                        usleep(1000000);
#endif
                    }
                    std::cout << std::endl << "    Reboot finished with result: "
                        << pDevice->GetUpdateNode("RebootResult")->GetValue() << std::endl;
                } else {
                    std::cout << "    Reboot is not supported by this device, please restart it manually." << std::endl;
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

    std::cout << "  " << std::endl;
    std::cout << "DEVICE DISABLE UPDATE MODE" << std::endl;
    std::cout << "##########################" << std::endl << std::endl;

    try {
        // CHECK IF UPDATE MODE IS ACTIVE
        if (pDevice->IsUpdateModeActive()) {
            // SET DEVICE TO NORMAL OPERATING
            pDevice->SetUpdateMode(false, "");
        }
        std::cout << "UpdateMode is: " << (pDevice->IsUpdateModeActive() ? "On" : "Off") << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    std::cout << "  " << std::endl;
    std::cout << "RELEASE" << std::endl;
    std::cout << "#######" << std::endl << std::endl;

    // Release buffers
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
