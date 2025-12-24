// Copyright 2020 Baumer Optronic

#if defined(_WIN32)
#include <windows.h>
#include <conio.h>
#endif
#include <iostream>
#include <string>
#include <vector>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

BGAPI2::SystemList* systemList = NULL;
BGAPI2::System* pSystem = NULL;
BGAPI2::InterfaceList* interfaceList = NULL;
BGAPI2::Interface* pInterface = NULL;
BGAPI2::DeviceList* deviceList = NULL;
BGAPI2::Device* pDevice = NULL;

int open_cam() {
    int result = -1;
    try {
        pDevice = NULL;
        systemList = BGAPI2::SystemList::GetInstance();
        systemList->Refresh();
        for (BGAPI2::SystemList::iterator sysIterator = systemList->begin();
            sysIterator != systemList->end() && !pDevice;
            sysIterator++) {
            sysIterator->Open();
            pSystem = (*systemList)[sysIterator->GetID()];

            interfaceList = sysIterator->GetInterfaces();
            interfaceList->Refresh(100);  // timeout of 100 msec
            for (BGAPI2::InterfaceList::iterator ifIterator = interfaceList->begin();
                ifIterator != interfaceList->end() && !pDevice;
                ifIterator++) {
                pInterface = (*interfaceList)[ifIterator->GetID()];
                pInterface->Open();

                deviceList = pInterface->GetDevices();
                deviceList->Refresh(1000);
                if (deviceList->size() > 0) {
                    for (BGAPI2::DeviceList::iterator devIterator = deviceList->begin();
                        devIterator != deviceList->end(); devIterator++) {
                        std::cout << "Device Model: " << devIterator->GetModel() << std::endl;
                        devIterator->Open();
                        pDevice = (*deviceList)[devIterator->GetID()];

                        if (pDevice->GetRemoteNodeList()->GetNodePresent("boSerialSelector")) {
                            if (pDevice->GetRemoteNodeList()->GetNodePresent("DeviceLinkHeartbeatMode")) {
                                pDevice->GetRemoteNode("DeviceLinkHeartbeatMode")->SetString("Off");
                            }
                            if (pDevice->GetRemoteNodeList()->GetNodePresent("GevHeartbeatTimeout")) {
                                pDevice->GetRemoteNode("GevHeartbeatTimeout")->SetInt(1000000000);
                            }
                            result = 0;
                            break;
                        } else {
                            pDevice->Close();
                            pDevice = NULL;
                        }
                    }
                }

                if (!pDevice) {
                    pInterface->Close();
                    pInterface = NULL;
                }
            }

            if (!pDevice) {
                pSystem->Close();
                pSystem = NULL;
            }
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        result = -1;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
    return result;
}

int close_cam() {
    int result = -1;
    try {
        if (pDevice) {
            pDevice->Close();
            pDevice = NULL;
        }
        if (pInterface) {
            pInterface->Close();
            pInterface = NULL;
        }
        if (pSystem) {
            pSystem->Close();
            pSystem = NULL;
        }
        BGAPI2::SystemList::ReleaseInstance();
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        result = -1;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
    return result;
}

// ------------------------------------------------------------------

void check_avail() {
    const char* msg = "?";
    if (pDevice) {
        BGAPI2::Node *node = pDevice->GetRemoteNodeList()->GetNode("boSerialSelector");
        if (node->GetAvailable()) {
            msg = "available (intern";
        } else {
            msg = "NOT available (extern";
        }
    }
    printf("serial feature are : %s serial port is active)\n", msg);
}

// ------------------------------------------------------------------

void BGAPI2CALL DevEventHandler(void* /*callbackOwner*/, BGAPI2::Events::DeviceEvent* pDevEvent) {
    if (NULL != pDevEvent) {
        std::cout << "Event received:" << pDevEvent->GetName().get() << " " <<
            pDevEvent->GetId().get() << " " << (bo_double)pDevEvent->GetTimeStamp() << std::endl;
    }
}

void start_handler_events() {
    try {
        pDevice->RegisterDeviceEvent(BGAPI2::Events::EVENTMODE_EVENT_HANDLER);
        pDevice->RegisterDeviceEventHandler(pDevice, (BGAPI2::Events::DeviceEventHandler) & DevEventHandler);
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        printf("ExceptionType: %s ErrorDescription: %s in function: %s\n",
            ex.GetType().get(), ex.GetErrorDescription().get(), ex.GetFunctionName().get());
    }
    printf("event handler started.\n");
}

void stop_handler_events() {
    try {
        pDevice->UnregisterDeviceEvent();
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        printf("ExceptionType: %s ErrorDescription: %s in function: %s\n",
            ex.GetType().get(), ex.GetErrorDescription().get(), ex.GetFunctionName().get());
    }
    printf("event handler stopped.\n");
}

// ------------------------------------------------------------------

void start_poll_events() {
    try {
        pDevice->RegisterDeviceEvent(BGAPI2::Events::EVENTMODE_POLLING);
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        printf("ExceptionType: %s ErrorDescription: %s in function: %s\n",
            ex.GetType().get(), ex.GetErrorDescription().get(), ex.GetFunctionName().get());
    }
    printf("event polling started.\n");
}

void stop_poll_events() {
    try {
        pDevice->UnregisterDeviceEvent();
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        printf("ExceptionType: %s ErrorDescription: %s in function: %s\n",
            ex.GetType().get(), ex.GetErrorDescription().get(), ex.GetFunctionName().get());
    }
    printf("event polling stopped.\n");
}

void poll_events() {
    if (pDevice->GetEventMode() == BGAPI2::Events::EVENTMODE_POLLING) {
        BGAPI2::Events::DeviceEvent DeviceEvent;
        if (pDevice->GetDeviceEvent(&DeviceEvent, 100)) {
            std::string sEventId = "";
            std::string sEventName = "";
            bo_double timestamp = 0.0;
            try { sEventId = DeviceEvent.GetId().get(); }
            catch (BGAPI2::Exceptions::IException&) { sEventId = ""; }
            try { sEventName = DeviceEvent.GetName().get(); }
            catch (BGAPI2::Exceptions::IException&) { sEventName = ""; }
            try { timestamp = (bo_double)DeviceEvent.GetTimeStamp(); }
            catch (BGAPI2::Exceptions::IException&) { timestamp = 0.0; }
            printf(" EventID: %s  %s Timestamp: %f\n", sEventId.c_str(), sEventName.c_str(), timestamp);
        } else {
            printf(" no event.\n");
        }
    } else {
        printf("no polling initialized!\n");
    }
}

void generate_event() {
    if (pDevice != NULL) {
        if (pDevice->GetRemoteNodeList()->GetNodePresent("TestEventGenerate")) {  // raspi + gevserver
            pDevice->GetRemoteNode("TestEventGenerate")->Execute();
            printf("test event generated.\n");
        } else {  // VLXT-C31.I
            if (pDevice->GetRemoteNodeList()->GetNodePresent("LineSelector")) {
                pDevice->GetRemoteNode("LineSelector")->SetString("Line4");
                pDevice->GetRemoteNode("LineSource")->SetString("UserOutput1");
                pDevice->GetRemoteNode("UserOutputSelector")->SetString("UserOutput1");
                pDevice->GetRemoteNode("EventSelector")->SetString("Line4RisingEdge");
                pDevice->GetRemoteNode("EventNotification")->SetString("On");

                pDevice->GetRemoteNode("UserOutputValue")->SetBool(true);
                pDevice->GetRemoteNode("UserOutputValue")->SetBool(false);
            }
        }
    }
}

void get_event_mode() {
    if (pDevice != NULL) {
        switch (pDevice->GetEventMode()) {
        case BGAPI2::Events::EVENTMODE_POLLING: printf("event mode: polling\n"); break;
        case BGAPI2::Events::EVENTMODE_EVENT_HANDLER: printf("event mode: handler\n"); break;
        case BGAPI2::Events::EVENTMODE_UNREGISTERED: printf("event mode: unregistered\n"); break;
        default: printf("event mode: unknown\n"); break;
        }
    }
}

// ------------------------------------------------------------------

std::string get_serial_portname() {
    std::vector<std::string> names;
    const char* nodename = "boSerialSelector";
    int   i;

    if (pDevice != NULL && pDevice->GetRemoteNodeList()->GetNodePresent(nodename)) {
        for (i = 0;
            i < (int)(pDevice->GetRemoteNode(nodename)->GetEnumNodeList()->GetNodeCount());
            i++) {
            std::string name = pDevice->GetRemoteNode(nodename)->GetEnumNodeList()->GetNodeByIndex(i)->GetName().get();
            names.push_back(name);
        }
        i = 1;
        std::vector<std::string>::iterator name;
        for (name = names.begin(); name != names.end(); name++) {
            printf("  %d. %s\n", i, name->c_str());
            i++;
        }
        if (names.size() == 1) {
            return names[0].c_str();
        } else {
            printf("enter number:");
#if defined(_WIN32)
            int c = _getch();
#else
            int c = 0;
#endif            
            i = c - '1';
            if (i >= 0 && i < (int)names.size()) {
                printf("ok, use %d (%s)\n", i+1, names[i].c_str());
                return names[i];
            }
        }
    }
    return "";
}

void set_extern_serial(const std::string portname) {
    try {
        const char* comport = "";
        if (portname == "UART0") comport = "COM7";
        if (portname == "UART1") comport = "COM5";
        pDevice->SetSerialPort(portname.c_str(), comport);
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
}

void set_intern_serial(const std::string portname) {
    try {
        pDevice->SetSerialPort(portname.c_str(), "");
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }
}

void set_serial_selector(char* uartname) {
    if (pDevice != NULL && pDevice->GetRemoteNodeList()->GetNodePresent("boSerialSelector")) {
        BGAPI2::Node *node = pDevice->GetRemoteNodeList()->GetNode("boSerialSelector");
        node->SetValue(uartname);
        printf("serial selector is %s\n", uartname);
    }
}

void test_feature_access(void) {
    if (pDevice != NULL && pDevice->GetRemoteNodeList()->GetNodePresent("boSerialSelector")) {
        BGAPI2::Node *selnode = pDevice->GetRemoteNodeList()->GetNode("boSerialSelector");
        BGAPI2::String uartname = selnode->GetValue();

        BGAPI2::NodeMap *selnodemap = selnode->second->GetSelectedNodeList();
        BGAPI2::NodeMap::iterator feature;
        for (feature = selnodemap->begin(); feature != selnodemap->end(); feature++) {
            BGAPI2::NodeMap::iterator selected_node = pDevice->GetRemoteNodeList()->find(feature->GetName());
            if (selected_node != pDevice->GetRemoteNodeList()->end()) {
                std::string s, n;
                try {
                    if (selected_node->GetAvailable()) {
                        s = selected_node->GetCurrentAccessMode().get();
                        n = selected_node->GetName();
                    }
                }
                catch (...) {}
                printf(" %s:  %s = %s\n", uartname.get(), n.c_str(), s.c_str());
                // break;
            }
        }
    }
}

// ------------------------------------------------------------------

void usage() {
    printf("SerialPort tester.\n");
    printf(" E   extern serial port. (for extern program usage)\n");
    printf(" I   intern serial port. (genicam features)\n");
    printf(" a   check serial feature availability\n");
    printf(" s   start events (handler)\n");
    printf(" t   stop events (handler)\n");
    printf(" P   start events (polling)\n");
    printf(" O   stop events (polling)\n");
    printf(" p   poll event\n");
    printf(" g   generate test event\n");
    printf(" m   get event mode\n");
    printf(" q   quit program\n");
    printf(" h   this help\n");
}

int main() {
    int result = 0;
    if (open_cam() == 0) {
        // pDevice is now valid
        try {
            if (pDevice->GetRemoteNodeList()->GetNodePresent("EventSelector") &&
                pDevice->GetRemoteNode("EventSelector")->GetEnumNodeList()->GetNodePresent("Test")) {
                pDevice->GetRemoteNode("EventSelector")->SetString("Test");
                if ((pDevice->GetRemoteNode("EventNotification")->GetEnumNodeList()->GetNodePresent("Off")) == true) {
                    pDevice->GetRemoteNode("EventNotification")->SetString("Off");
                }
            }
#if defined(_WIN32)
            bool running = true;
            while (running) {
                ::Sleep(10);
                if (_kbhit()) {
                    int c = _getch();
                    switch (c) {
                    case 'E': set_extern_serial(get_serial_portname()); break;
                    case 'I': set_intern_serial(get_serial_portname()); break;
                    case 'a': check_avail(); break;

                    case '0': set_serial_selector("UART0"); break;
                    case '1': set_serial_selector("UART1"); break;
                    case 'F': test_feature_access();  break;

                    case 's': start_handler_events(); break;
                    case 't': stop_handler_events(); break;

                    case 'P': start_poll_events(); break;
                    case 'O': stop_poll_events(); break;
                    case 'p': poll_events(); break;
                    case 'g': generate_event(); break;
                    case 'm': get_event_mode(); break;

                    case 'q': running = false; break;
                    default:
                    case 'h': usage(); break;
                    }
                    // check_cam();
                }
            }
#endif
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            result = -1;
            printf("ExceptionType: %s ErrorDescription: %s in function: %s\n",
                ex.GetType().get(), ex.GetErrorDescription().get(), ex.GetFunctionName().get());
        }
    }
    close_cam();
    return result;
}
