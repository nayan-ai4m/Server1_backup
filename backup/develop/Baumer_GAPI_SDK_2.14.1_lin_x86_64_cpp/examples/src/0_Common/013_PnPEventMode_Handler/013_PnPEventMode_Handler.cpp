/* Copyright 2019-2020 Baumer Optronic */
#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#if defined(_WIN32)
#include <windows.h>
#include <conio.h>
#else
#include <unistd.h>
#include <termios.h>
#include <stdlib.h>
#endif
#include "bgapi2_genicam/bgapi2_genicam.hpp"

// WARNING ACCESS IS NOT THREAD SAVE - YOU SHOULD ALWAYS USE A LOCK (LIKE MUTEX) TO ACCESS THEM FROM DIFFERENT THREADS
// This example does not use std::mutex to support old compiler without C++11
bool gDisplayDeviceLists = true;

// CALLBACK FUNCTION DEFINITION
void BGAPI2CALL PnPEventHandler( void * callbackOwner, BGAPI2::Events::PnPEvent * pPnPEvent);

int main(int argc, char* argv[]) {
    BGAPI2::SystemList *systemList = NULL;
    BGAPI2::System * pSystemGigE = NULL;
    BGAPI2::System * pSystemUSB3 = NULL;
    BGAPI2::InterfaceList * interfaceListGigE = NULL;
    BGAPI2::InterfaceList * interfaceListUSB3 = NULL;
    BGAPI2::Interface * pInterface = NULL;
    BGAPI2::DeviceList *deviceList = NULL;
    std::string upTime = (argc > 1) ? argv[1] : "200";
    int waitCounter = atoi(upTime.c_str());
    int returncode = 0;

    std::cout << "+------------------------------+" << std::endl;
    std::cout << "| 013_PnPEventMode_Handler.cpp |" << std::endl;
    std::cout << "+------------------------------+" << std::endl;
    std::cout << " " << std::endl;

    try {
        systemList = BGAPI2::SystemList::GetInstance();
        systemList->Refresh();

        for (BGAPI2::SystemList::iterator sysIterator = systemList->begin();
            sysIterator != systemList->end();
            sysIterator++) {
            std::cout << "  System Filename : " << sysIterator->GetFileName() << std::endl;
            std::cout << "  System Version  : " << sysIterator->GetVersion() << std::endl;
            std::cout << "  System Path     : " << sysIterator->GetPathName() << std::endl;
            if (sysIterator->GetTLType() == "GEV") {
                if (pSystemGigE == NULL) {
                    pSystemGigE = *sysIterator;
                    pSystemGigE->Open();
                    std::cout << "  System GigE     : opened" << std::endl;
                }
            }

            if (sysIterator->GetTLType() == "U3V") {
                if (pSystemUSB3 == NULL) {
                    pSystemUSB3 = *sysIterator;
                    pSystemUSB3->Open();
                    std::cout << "  System USB3     : opened" << std::endl;
                }
            }
            std::cout << std::endl;
        }
        std::cout << std::endl;


        if (pSystemGigE != NULL) {
            // INIT PNP EVENTS GigE
            std::cout << pSystemGigE->GetDisplayName() << std::endl;
            interfaceListGigE = pSystemGigE->GetInterfaces();
            interfaceListGigE->Refresh(100);
            for (BGAPI2::InterfaceList::iterator ifIterator = interfaceListGigE->begin();
                ifIterator != interfaceListGigE->end();
                ifIterator++) {
                pInterface = *ifIterator;
                BGAPI2::InterfaceList::iterator checkLastIterator = ifIterator;
                checkLastIterator++;
                pInterface->Open();
                deviceList = pInterface->GetDevices();
                deviceList->Refresh(100);

                std::cout << " |" << std::endl;
                std::cout << " +-- " << pInterface->GetDisplayName() << " opened" << std::endl;

                if (checkLastIterator != interfaceListGigE->end())
                    std::cout << " |";
                else
                    std::cout << "  ";

                pInterface->RegisterPnPEvent(BGAPI2::Events::EVENTMODE_EVENT_HANDLER);
                BGAPI2::Events::EventMode currentEventMode = pInterface->GetEventMode();
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
                std::cout << "     Register Event Mode to: " << sCurrentEventMode << std::endl;

                // CALLBACK FUNCTION set
                pInterface->RegisterPnPEventHandler(pInterface, (BGAPI2::Events::PnPEventHandler) &PnPEventHandler);

            }
        }

        if (pSystemUSB3 != NULL) {
            // INIT PNP EVENTS USB3
            std::cout << std::endl << std::endl;
            std::cout << pSystemUSB3->GetDisplayName() << std::endl;
            interfaceListUSB3 = pSystemUSB3->GetInterfaces();
            interfaceListUSB3->Refresh(100);
            for (BGAPI2::InterfaceList::iterator ifIterator = interfaceListUSB3->begin();
                ifIterator != interfaceListUSB3->end();
                ifIterator++) {
                pInterface = *ifIterator;
                BGAPI2::InterfaceList::iterator checkLastIterator = ifIterator;
                checkLastIterator++;
                pInterface->Open();
                deviceList = pInterface->GetDevices();
                deviceList->Refresh(100);

                std::cout << " |" << std::endl;
                std::cout << " +-- " << pInterface->GetDisplayName() << " opened" << std::endl;
                if (checkLastIterator != interfaceListUSB3->end())
                    std::cout << " |";
                else
                    std::cout << "  ";

                pInterface->RegisterPnPEvent(BGAPI2::Events::EVENTMODE_EVENT_HANDLER);

                BGAPI2::Events::EventMode currentEventMode = pInterface->GetEventMode();
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
                std::cout << "     Register Event Mode to: " << sCurrentEventMode << std::endl;

                // CALLBACK FUNCTION set
                pInterface->RegisterPnPEventHandler(pInterface, (BGAPI2::Events::PnPEventHandler) &PnPEventHandler);
            }
            std::cout << std::endl;
        }
        std::cout << std::endl;

        while (0 < waitCounter) {
            // WARNING ACCESS IS NOT THREAD SAVE - YOU SHOULD ALWAYS USE A LOCK (LIKE MUTEX)
            // TO ACCESS THEM FROM DIFFERENT THREADS
            // This example does not use std::mutex to support old compiler without C++11
            if (gDisplayDeviceLists == true) {

                if (pSystemGigE != NULL) {
                    // LIST UP GigE DEVICES
                    std::cout << std::endl << std::endl;
                    std::cout << pSystemGigE->GetDisplayName() << std::endl;
                    for (BGAPI2::InterfaceList::iterator ifIterator = interfaceListGigE->begin();
                        ifIterator != interfaceListGigE->end();
                        ifIterator++) {
                        pInterface = *ifIterator;
                        BGAPI2::InterfaceList::iterator checkLastIterator = ifIterator;
                        checkLastIterator++;
                        deviceList = pInterface->GetDevices();
                        deviceList->Refresh(100);

                        std::cout << " |" << std::endl;
                        std::cout << " +-- " << pInterface->GetDisplayName() << " (devices: "
                            << deviceList->size() << ")" << " IP "
                            << pInterface->GetNode("GevInterfaceSubnetIPAddress")->GetValue() << " Mask "
                            << pInterface->GetNode("GevInterfaceSubnetMask")->GetValue() << std::endl;

                        if (deviceList->size() > 0) {
                            for (BGAPI2::DeviceList::iterator devIterator = deviceList->begin();
                                devIterator != deviceList->end();
                                devIterator++) {
                                BGAPI2::DeviceList::iterator checkLastDeviceIterator = devIterator;
                                checkLastDeviceIterator++;
                                if (checkLastIterator != interfaceListGigE->end()) {
                                    std::cout << " |    |" << std::endl;
                                    std::cout << " |    +-- ";
                                } else {
                                    std::cout << "      |" << std::endl;
                                    std::cout << "      +-- ";
                                }

                                std::cout << std::setw(8) << devIterator->GetModel() << " ("
                                    << devIterator->GetSerialNumber() << ")" << " Access "
                                    << devIterator->GetAccessStatus() << " IP "
                                    << devIterator->GetNodeList()->GetNode("GevDeviceIPAddress")->GetValue()
                                    << " Mask "
                                    << devIterator->GetNodeList()->GetNode("GevDeviceSubnetMask")->GetValue()
                                    << std::endl;

                                if ((checkLastIterator != interfaceListGigE->end()) && (checkLastDeviceIterator == deviceList->end())) {
                                    std::cout << " |" << std::endl;
                                }
                            }
                        } else {
                            if (checkLastIterator != interfaceListGigE->end()) {
                                std::cout << " |    " << std::endl;
                            } else {
                                std::cout << "      " << std::endl;
                            }
                        }
                    }
                }
                if (pSystemUSB3 != NULL) {
                    // LIST UP USB3 DEVICES
                    std::cout << std::endl << std::endl;
                    std::cout << pSystemUSB3->GetDisplayName() << std::endl;
                    for (BGAPI2::InterfaceList::iterator ifIterator = interfaceListUSB3->begin();
                        ifIterator != interfaceListUSB3->end();
                        ifIterator++) {
                        pInterface = *ifIterator;
                        deviceList = pInterface->GetDevices();
                        deviceList->Refresh(100);
                        BGAPI2::InterfaceList::iterator checkLastIterator = ifIterator;
                        checkLastIterator++;
                        std::cout << " |" << std::endl;
                        std::cout << " +-- " << pInterface->GetDisplayName() << " (devices: "
                            << deviceList->size() << ")" << std::endl;

                        if (deviceList->size() > 0) {
                            for (BGAPI2::DeviceList::iterator devIterator = deviceList->begin();
                                devIterator != deviceList->end();
                                devIterator++) {
                                BGAPI2::DeviceList::iterator checkLastDeviceIterator = devIterator;
                                checkLastDeviceIterator++;
                                if (checkLastIterator != interfaceListUSB3->end()) {
                                    std::cout << " |    |" << std::endl;
                                    std::cout << " |    +-- ";
                                } else {
                                    std::cout << "      |" << std::endl;
                                    std::cout << "      +-- ";
                                }
                                std::cout << std::setw(8) << devIterator->GetModel() << " ("
                                    << devIterator->GetSerialNumber() << ")" << " Access "
                                    << devIterator->GetAccessStatus() << std::endl;
                                if ((checkLastIterator != interfaceListUSB3->end()) &&
                                    (checkLastDeviceIterator == deviceList->end())) {
                                    std::cout << " |" << std::endl;
                                }
                            }
                        } else {
                            if (checkLastIterator != interfaceListUSB3->end()) {
                                std::cout << " |    " << std::endl;
                            } else {
                                std::cout << "      " << std::endl;
                            }
                        }
                    }
                    std::cout << std::endl;
                }

                std::cout << std::endl;
                // WARNING ACCESS IS NOT THREAD SAVE - YOU SHOULD ALWAYS USE A LOCK (LIKE MUTEX)
                // TO ACCESS THEM FROM DIFFERENT THREADS
                // This example does not use std::mutex to support old compiler without C++11
                gDisplayDeviceLists = false;

            }  // end of if(gDisplayDeviceLists == true)

            else {
                std::cout << "." << std::flush;
#if defined(_WIN32)
                Sleep(200);
#else
                usleep(200000);
#endif
            }

            waitCounter--;

            if (0 >= waitCounter) {
                std::cout << std::endl << "Input 'c' to continue or any other value to stop." << std::endl;
                std::string sInput;
                std::cin >> sInput;
                waitCounter = (sInput == "c") ? 100 : 0;
            }

        }  // end of while(0<waitCounter)
        std::cout << std::endl;


        if (pSystemGigE != NULL) {
            // RESET EVENT MODE TO UNREGISTERED AND INTERFACE CLOSE GigE
            std::cout << std::endl << std::endl;
            std::cout << pSystemGigE->GetDisplayName() << std::endl;
            for (BGAPI2::InterfaceList::iterator ifIterator = interfaceListGigE->begin();
                ifIterator != interfaceListGigE->end();
                ifIterator++) {
                pInterface = *ifIterator;
                BGAPI2::InterfaceList::iterator checkLastIterator = ifIterator;
                checkLastIterator++;
                std::cout << " |" << std::endl;
                std::cout << " +-- " << pInterface->GetDisplayName() << " closing " << std::endl;

                if (checkLastIterator != interfaceListGigE->end())
                    std::cout << " |";
                else
                    std::cout << "  ";

                pInterface->UnregisterPnPEvent();
                BGAPI2::Events::EventMode currentEventMode = pInterface->GetEventMode();
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
                std::cout << "     Unregister Event Mode to:   " << sCurrentEventMode << std::endl;
                pInterface->Close();
            }
        }

        if (pSystemUSB3 != NULL) {
            // RESET EVENT MODE TO UNREGISTERED AND INTERFACE CLOSE USB3
            std::cout << std::endl << std::endl;
            std::cout << pSystemUSB3->GetDisplayName() << std::endl;
            for (BGAPI2::InterfaceList::iterator ifIterator = interfaceListUSB3->begin();
                ifIterator != interfaceListUSB3->end();
                ifIterator++) {
                pInterface = *ifIterator;
                BGAPI2::InterfaceList::iterator checkLastIterator = ifIterator;
                checkLastIterator++;
                std::cout << " |" << std::endl;
                std::cout << " +-- " << pInterface->GetDisplayName() << " closing" << std::endl;
                if (checkLastIterator != interfaceListUSB3->end())
                    std::cout << " |";
                else
                    std::cout << "  ";
                pInterface->UnregisterPnPEvent();
                BGAPI2::Events::EventMode currentEventMode = pInterface->GetEventMode();
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
                std::cout << "     Unregister Event Mode to:   " << sCurrentEventMode << std::endl;
            }
        }
        std::cout << std::endl;

        if (pSystemGigE != NULL) {
            pSystemGigE->Close();
        }
        if (pSystemUSB3 != NULL) {
            pSystemUSB3->Close();
        }
        BGAPI2::SystemList::ReleaseInstance();
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "Error in function: " << ex.GetFunctionName() << std::endl << "Error description: "
            << ex.GetErrorDescription() << std::endl << std::endl;
        try {
            if (pSystemGigE != NULL) {
                // RESET EVENT MODE TO UNREGISTERED AND INTERFACE CLOSE GigE
                for (BGAPI2::InterfaceList::iterator ifIterator = interfaceListGigE->begin();
                    ifIterator != interfaceListGigE->end();
                    ifIterator++) {
                    pInterface = *ifIterator;
                    pInterface->UnregisterPnPEvent();
                    pInterface->Close();
                }
            }

            if (pSystemUSB3 != NULL) {
                // RESET EVENT MODE TO UNREGISTERED AND INTERFACE CLOSE USB3
                for (BGAPI2::InterfaceList::iterator ifIterator = interfaceListUSB3->begin();
                    ifIterator != interfaceListUSB3->end();
                    ifIterator++) {
                    pInterface = *ifIterator;
                    pInterface->UnregisterPnPEvent();
                    pInterface->Close();
                }
            }

            if (pSystemGigE != NULL) {
                pSystemGigE->Close();
            }
            if (pSystemUSB3 != NULL) {
                pSystemUSB3->Close();
            }
            BGAPI2::SystemList::ReleaseInstance();
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "Error in function: " << ex.GetFunctionName() << std::endl << "Error description: "
                << ex.GetErrorDescription() << std::endl << std::endl;
        }
    }
    std::cout << "Input any number to close the program:";
    int endKey = 0;
    std::cin >> endKey;
    return returncode;
}

// CALLBACK
void BGAPI2CALL PnPEventHandler(void * callbackOwner, BGAPI2::Events::PnPEvent * pPnPEvent) {
    if (NULL != pPnPEvent) {
        std::cout << std::endl;
        std::cout << std::endl;
        std::cout << " [callback of " << ((BGAPI2::Interface *)callbackOwner)->GetDisplayName() << "] ";
        std::cout << " EventID " << pPnPEvent->GetId() << " PnPType: "
            << ((pPnPEvent->GetPnPType() == 0) ? "removed" : "added  ")
            << " SerialNumber: " << pPnPEvent->GetSerialNumber() << std::endl;
        std::cout << std::endl;

#if defined(_WIN32)
        Sleep(1000);
#else
        usleep(1000000);
#endif
        // WARNING ACCESS IS NOT THREAD SAVE - YOU SHOULD ALWAYS USE A LOCK (LIKE MUTEX)
        // TO ACCESS THEM FROM DIFFERENT THREADS
        // This example does not use std::mutex to support old compiler without C++11
        gDisplayDeviceLists = true;  // enable refresh of device list display
    } else {
        std::cout << " [callback] - received invalid Interface Event! " << std::endl;
    }
    return;
}
