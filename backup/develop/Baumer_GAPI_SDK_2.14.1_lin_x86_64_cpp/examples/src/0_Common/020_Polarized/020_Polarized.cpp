/* Copyright 2019-2020 Baumer Optronic */
/*
    This example describes how to obtain polarisation data from the Baumer VCXU-50MP and VCXG-50MP.
    The example describes all how to use the provided Baumer GAPI API functionality to configure
    the camera and calculate the required polarisation data (AOL, DOP, ADOLP, Intensity)
*/

#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <utility>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

// handles the command line argument parsing
#include "Arguments.h"

static std::set<std::string> sComponents;
static std::map<std::string, BGAPI2::Polarizer::Formats> supportedComponents;

static double g_aopOffset = 0.0;
static bool   g_bAopOffset = false;

struct DeviceMatch {
    BGAPI2::System* pSystem;
    BGAPI2::Interface* pInterface;
    BGAPI2::Device* pDevice;
};

// Get all the supported components from the polarizer
static void GetSupportedComponents(BGAPI2::Polarizer* const polarizer,
    const bool is_color,
    std::string* const help);

// Get the Angle Offset from the command line parameter (if provided) and use it for the calculation
static void argumentAopOffset(const Argument& argument, const ArgumentMode mode, const char* const pParam);

// Get the required components from the command line argument
static void argumentComponent(const Argument& argument, const ArgumentMode mode, const char* const pParam);

// connect to the first polarisation camera found on the system
static int GetFirstDevice(DeviceMatch* const pMatch,
    bool(*pSystemFilter)(BGAPI2::System* pSystem),
    bool(*pInterfaceFilter)(BGAPI2::Interface* pInterface),
    bool(*pDeviceFilter)(BGAPI2::Device* pDevice),
    std::ostream* log);

// Helper to Display various information of the camera
static void GetDeviceInfo(std::ostream* log, BGAPI2::Device* const pDevice, const bool bOpen);

// Helper to filter found cameras devices and select only polarization camera for this example
static bool PolarizationDeviceFilter(BGAPI2::Device* const pDevice);

// Release all allocated resources
static int ReleaseAllResources(BGAPI2::System* pSystem,
    BGAPI2::Interface* pInterface,
    BGAPI2::Device* pDevice,
    BGAPI2::DataStream* pDataStream,
    std::map<BGAPI2::Polarizer::Formats, BGAPI2::Image*>* requestedComponents);

int main(int argc, char* argv[]) {
    // Declaration of variables
    BGAPI2::System* pSystem = NULL;
    BGAPI2::Interface* pInterface = NULL;
    BGAPI2::Device* pDevice = NULL;

    BGAPI2::DataStreamList *datastreamList = NULL;
    BGAPI2::DataStream * pDataStream = NULL;
    BGAPI2::String sDataStreamID;

    BGAPI2::BufferList *bufferList = NULL;
    BGAPI2::Buffer * pBuffer = NULL;
    BGAPI2::String sBufferID;
    std::map<BGAPI2::Polarizer::Formats, BGAPI2::Image*> requestedComponents;

    int returncode = 0;

    bool bBufferedLog = true;


    std::cout << std::endl;
    std::cout << "################################################" << std::endl;
    std::cout << "# PROGRAMMER'S GUIDE Example 020_Polarized.cpp #" << std::endl;
    std::cout << "################################################" << std::endl;
    std::cout << std::endl << std::endl;

    BGAPI2::Polarizer polarizer;
    BGAPI2::ImageProcessor imgProc;

    // Find and open the first polarisation camera
    DeviceMatch match = { NULL, NULL, NULL };
    std::stringstream bufferedLog;

    const int code = GetFirstDevice(
        &match,
        NULL,
        NULL,
        PolarizationDeviceFilter,
        bBufferedLog ? &bufferedLog : &std::cout);

    pSystem = match.pSystem;
    pInterface = match.pInterface;
    pDevice = match.pDevice;

    if ((code != 0) || (pDevice == NULL)) {
        // Error or no device found
        if (bBufferedLog != false) {
            // Display full device search
            std::cout << bufferedLog.str();
        }

        returncode = (returncode == 0) ? code : returncode;
        if (returncode == 0) {
            std::cout << " No Polarized Device found " << sDataStreamID << std::endl;
        }

        std::cout << std::endl << "End" << std::endl << "Input any number to close the program:";
        int endKey = 0;
        std::cin >> endKey;

        ReleaseAllResources(pSystem, pInterface, pDevice, pDataStream, &requestedComponents);
        return returncode;
    }


    std::string help;

    bool is_color = false;
    BGAPI2::Polarizer::IsPolarized(pDevice, &is_color);

    GetSupportedComponents(&polarizer, is_color, &help);
    help = "enable component (" + help + ")";

    bool enableInterpolation = false;

    static const Argument argumentList[] = {
        { &enableInterpolation, "i", "interpolate", true, argumentBoolFlag, false, "<on/off>",
            "enable or disable interpolation" },
        { NULL, "c", "component", false, argumentComponent, 0, "<component>", help.c_str() },
        { NULL, "o", "offsetAOP", false, argumentAopOffset, 0, "<aop offset>", "angle of polarization offset" },
    };

    parseArguments(argumentList, sizeof(argumentList) / sizeof(argumentList[0]), argc, argv);

    GetDeviceInfo(&std::cout, pDevice, false);

    std::cout << "DEVICE PARAMETER SETUP" << std::endl;
    std::cout << "######################" << std::endl << std::endl;

    // Set angle of polarization offset to device, if command line parameter passed
    if (g_bAopOffset) {
        try {
            BGAPI2::Node* const pAngleOfPolarizationOffset =
                pDevice->GetRemoteNode("CalibrationAngleOfPolarizationOffset");
            pAngleOfPolarizationOffset->SetDouble(g_aopOffset);
            std::cout << "         AngleOfPolarizationOffset:  "
                << pAngleOfPolarizationOffset->GetValue() << std::endl;
        }
        catch (BGAPI2::Exceptions::IException& ex) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
            std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
            std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        }
    }

    try {
        // Set trigger mode to off (FreeRun)
        pDevice->GetRemoteNode("TriggerMode")->SetString("Off");
        std::cout << "         TriggerMode:             "
            << pDevice->GetRemoteNode("TriggerMode")->GetValue() << std::endl;
        std::cout << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    std::cout << std::endl;
    std::cout << "POLARIZER CONFIGURATION" << std::endl;
    std::cout << "#######################" << std::endl << std::endl;

    try {
        // Enable or disable interpolation
        polarizer.EnableInterpolation(enableInterpolation);
        std::cout << "Interpolation " << (enableInterpolation ? "on" : "off") << std::endl;

        // Configure the polarizer to use the calibration values from the camera device
        polarizer.ReadCalibrationData(pDevice);
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    // Enable requested polarisation formats and create image containers
    try {
        for (std::set<std::string>::const_iterator component = sComponents.begin();
            component != sComponents.end(); component++) {
            std::map<std::string, BGAPI2::Polarizer::Formats>::const_iterator c = supportedComponents.find(*component);
            if (c == supportedComponents.end()) {
                std::cout << *component << ":" << " not supported" << std::endl;
                returncode = (returncode == 0) ? 1 : returncode;
            } else if (requestedComponents.find(c->second) == requestedComponents.end()) {
                polarizer.Enable(c->second, true);
                std::cout << *component << ":" << " enabled" << std::endl;

                requestedComponents.insert(
                    std::pair<BGAPI2::Polarizer::Formats, BGAPI2::Image*>(c->second, imgProc.CreateImage()));
            }
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    if (returncode) {
        ReleaseAllResources(pSystem, pInterface, pDevice, pDataStream, &requestedComponents);
        return returncode;
    }



    std::cout << "DATA STREAM LIST" << std::endl;
    std::cout << "################" << std::endl << std::endl;

    try {
        // Get information for all available data streams
        datastreamList = pDevice->GetDataStreams();
        datastreamList->Refresh();
        std::cout << "5.1.8   Detected datastreams:     " << datastreamList->size() << std::endl;

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

    // Open the first datastream
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
        ReleaseAllResources(pSystem, pInterface, pDevice, pDataStream, &requestedComponents);
        return returncode;
    }


    std::cout << "BUFFER LIST" << std::endl;
    std::cout << "###########" << std::endl << std::endl;

    try {
        // get the BufferList
        bufferList = pDataStream->GetBufferList();

        // allocate 4 buffers using internal buffer mode
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

    // Start DataStream acquisition
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

    // Start aquisition from camera device
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

    // Capture 12 images
    std::cout << " " << std::endl;
    std::cout << "CAPTURE 12 IMAGES BY IMAGE POLLING" << std::endl;
    std::cout << "##################################" << std::endl << std::endl;

    try {
        // Set to true to save result of the first captured image as a Baumer RAW image
        bool bSaveBrw = true;

        for (int i = 0; i < 12; i++) {
            BGAPI2::Buffer* pBufferFilled = pDataStream->GetFilledBuffer(1000);  // timeout 1000 msec
            if (pBufferFilled == NULL) {
                std::cout << "Error: Buffer Timeout after 1000 msec" << std::endl;
            } else {
                // Initialize the polarizer with the povided Buffer and calculate the requested formats.
                bool getResult = false;
                try {
                    polarizer.Initialize(pBufferFilled);
                    getResult = true;
                }
                catch (BGAPI2::Exceptions::IException& ex) {
                    std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
                    std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                    std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
                }

                // Queue buffer again
                pBufferFilled->QueueBuffer();

                // Get the calculated components
                if (getResult) {
                    try {
                        std::map<BGAPI2::Polarizer::Formats, BGAPI2::Image*>::iterator component;
                        for (component = requestedComponents.begin();
                            component != requestedComponents.end(); component++) {
                            std::string componentName = polarizer.GetFormatString(component->first).get();
                            std::cout << "  single component: " << componentName.c_str() << std::endl;

                            polarizer.Get(component->first, component->second);

                            if (bSaveBrw) {
                                std::string filename = componentName + ".brw";
                                component->second->GetNode("SaveBrw")->SetValue(filename.c_str());
                            }
                        }
                        bSaveBrw = false;
                    }
                    catch (BGAPI2::Exceptions::IException& ex) {
                        returncode = (returncode == 0) ? 1 : returncode;
                        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
                        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
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
    std::cout << " " << std::endl;

    std::cout << "CAMERA STOP" << std::endl;
    std::cout << "###########" << std::endl << std::endl;

    // Stop the camera
    try {
        if (pDevice->GetRemoteNodeList()->GetNodePresent("AcquisitionAbort")) {
            pDevice->GetRemoteNode("AcquisitionAbort")->Execute();
            std::cout << "5.1.12   " << pDevice->GetModel() << " aborted " << std::endl;
        }

        pDevice->GetRemoteNode("AcquisitionStop")->Execute();
        std::cout << "5.1.12   " << pDevice->GetModel() << " stopped " << std::endl;
        std::cout << std::endl;
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    // Stop DataStream acquisition
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

        ReleaseAllResources(pSystem, pInterface, pDevice, pDataStream, &requestedComponents);
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

void GetSupportedComponents(BGAPI2::Polarizer* const polarizer,
    const bool is_color,
    std::string* const help) {
    BGAPI2::Polarizer::formatlist list;
    for (BGAPI2::Polarizer::formatlist::const_iterator it = list.begin(); it != list.end(); it++) {
        if (BGAPI2::Polarizer::IsFormatAvailable(*it, is_color)) {
            std::string componentName = polarizer->GetFormatString(*it).get();
            supportedComponents.insert(std::pair<std::string, BGAPI2::Polarizer::Formats>(componentName, *it));
            if (help->length() > 0) {
                *help += "/";
            }
            *help += componentName;
        }
    }
}

// Get the required components from the command line argument
void argumentComponent(const Argument& /*argument*/, const ArgumentMode mode, const char* const pParam) {
    static bool bClearComponents = true;
    if (mode == eArgumentInit) {
        sComponents.clear();
        const char* components[] = { "Intensity", "AOP", "DOLP", "POL", "UNPOL", "ADOLP" };
        for (unsigned int i = 0; i < sizeof(components) / sizeof(components[0]); i++) {
            if (supportedComponents.find(components[i]) != supportedComponents.end()) {
                sComponents.insert(components[i]);
            }
        }
        bClearComponents = true;
    } else {
        if (bClearComponents) {
            sComponents.clear();
            bClearComponents = false;
        }
        if (pParam != NULL) {
            if (sComponents.find(pParam) == sComponents.end()) {
                sComponents.insert(pParam);
            }
        }
    }
}

// Get the Angle Offset from the command line parameter (if provided) and use it for the calculation
void argumentAopOffset(const Argument& /*argument*/, const ArgumentMode mode, const char* const pParam) {
    if (mode == eArgumentInit) {
        g_aopOffset = 0.0;
        g_bAopOffset = false;
    } else {
        double value = 0.0;
        int ret_value = 0;
#if defined(_WIN32)
        ret_value = sscanf_s(pParam, "%lf", &value);
#else
        ret_value = sscanf(pParam, "%lf", &value);
#endif
        if ((pParam != NULL) && (ret_value == 1)) {
            g_aopOffset = value;
            g_bAopOffset = true;
        }
    }
}

// Helper to filter found cameras devices and select only polarization camera for this example
bool PolarizationDeviceFilter(BGAPI2::Device* const pDevice) {
    return BGAPI2::Polarizer::IsPolarized(pDevice, NULL);
}

int GetFirstDevice(DeviceMatch* const pMatch,
    bool(*pSystemFilter)(BGAPI2::System* pSystem),
    bool(*pInterfaceFilter)(BGAPI2::Interface* pInterface),
    bool(*pDeviceFilter)(BGAPI2::Device* pDevice),
    std::ostream* log) {
    int returncode = 0;
    *log << "SYSTEM LIST" << std::endl;
    *log << "###########" << std::endl << std::endl;

    try {
        BGAPI2::SystemList* pSystemList = BGAPI2::SystemList::GetInstance();

        // Counting available systems (TL producers)
        pSystemList->Refresh();
        *log << "5.1.2   Detected systems:  " << pSystemList->size() << std::endl;

        // System device information
        for (BGAPI2::SystemList::iterator sysIterator = pSystemList->begin();
            sysIterator != pSystemList->end();
            sysIterator++) {
            BGAPI2::System* const pSystem = *sysIterator;
            *log << "  5.2.1   System Name:     " << pSystem->GetFileName() << std::endl;
            *log << "          System Type:     " << pSystem->GetTLType() << std::endl;
            *log << "          System Version:  " << pSystem->GetVersion() << std::endl;
            *log << "          System PathName: " << pSystem->GetPathName() << std::endl << std::endl;
        }

        for (BGAPI2::SystemList::iterator sysIterator = pSystemList->begin();
            sysIterator != pSystemList->end();
            sysIterator++) {
            *log << "SYSTEM" << std::endl;
            *log << "######" << std::endl << std::endl;

            BGAPI2::System* const pSystem = *sysIterator;
            pMatch->pSystem = pSystem;
            try {
                pSystem->Open();
                *log << "5.1.3   Open next system " << std::endl;
                *log << "  5.2.1   System Name:     " << pSystem->GetFileName() << std::endl;
                *log << "          System Type:     " << pSystem->GetTLType() << std::endl;
                *log << "          System Version:  " << pSystem->GetVersion() << std::endl;
                *log << "          System PathName: " << pSystem->GetPathName() << std::endl << std::endl;

                *log << "        Opened system - NodeList Information " << std::endl;
                *log << "          GenTL Version:   " << pSystem->GetNode("GenTLVersionMajor")->GetValue() << "."
                    << pSystem->GetNode("GenTLVersionMinor")->GetValue() << std::endl << std::endl;

                const char* pCloseSystemReason = "???";
                if ((pSystemFilter != NULL) && (pSystemFilter(pSystem) == false)) {
                    pCloseSystemReason = "skipped";
                } else {
                    *log << "INTERFACE LIST" << std::endl;
                    *log << "##############" << std::endl << std::endl;

                    try {
                        BGAPI2::InterfaceList* pInterfaceList = pSystem->GetInterfaces();
                        // Count available interfaces
                        pInterfaceList->Refresh(100);  // timeout of 100 msec
                        *log << "5.1.4   Detected interfaces: " << pInterfaceList->size() << std::endl;

                        // Interface information
                        for (BGAPI2::InterfaceList::iterator ifIterator = pInterfaceList->begin();
                            ifIterator != pInterfaceList->end();
                            ifIterator++) {
                            BGAPI2::Interface* const pInterface = *ifIterator;
                            *log << "  5.2.2   Interface ID:      " << pInterface->GetID() << std::endl;
                            *log << "          Interface Type:    " << pInterface->GetTLType() << std::endl;
                            *log << "          Interface Name:    " << pInterface->GetDisplayName() << std::endl
                                << std::endl;
                        }

                        *log << "INTERFACE" << std::endl;
                        *log << "#########" << std::endl << std::endl;

                        for (BGAPI2::InterfaceList::iterator ifIterator = pInterfaceList->begin();
                            ifIterator != pInterfaceList->end();
                            ifIterator++) {
                            try {
                                // Open the next interface in the list
                                BGAPI2::Interface* const pInterface = *ifIterator;
                                pMatch->pInterface = pInterface;
                                *log << "5.1.5   Open interface " << std::endl;
                                *log << "  5.2.2   Interface ID:      " << pInterface->GetID() << std::endl;
                                *log << "          Interface Type:    " << pInterface->GetTLType() << std::endl;
                                *log << "          Interface Name:    " << pInterface->GetDisplayName() << std::endl;

                                pInterface->Open();

                                const char* pReason = "???";
                                if ((pInterfaceFilter != NULL) && (pInterfaceFilter(pInterface) == false)) {
                                    pReason = "skipped";
                                } else {
                                    // Search for any camera is connected to this interface
                                    BGAPI2::DeviceList* const pDeviceList = pInterface->GetDevices();
                                    pDeviceList->Refresh(100);

                                    if (pDeviceList->size() == 0) {
                                        pReason = "no camera found";
                                    } else {
                                        *log << "   " << std::endl;
                                        *log << "        Opened interface - NodeList Information " << std::endl;
                                        if (pInterface->GetTLType() == "GEV") {
                                            *log << "          GevInterfaceSubnetIPAddress: "
                                                << pInterface->GetNode("GevInterfaceSubnetIPAddress")->GetValue()
                                                << std::endl;
                                            *log << "          GevInterfaceSubnetMask:      "
                                                << pInterface->GetNode("GevInterfaceSubnetMask")->GetValue()
                                                << std::endl;
                                        }
                                        if (pInterface->GetTLType() == "U3V") {
                                            // log << "          NodeListCount:     "
                                            // << pInterface->GetNodeList()->GetNodeCount() << std::endl;
                                        }

                                        // Open the first matching camera in the list
                                        try {
                                            // Counting available cameras
                                            *log << "5.1.6   Detected devices:         "
                                                << pDeviceList->size() << std::endl;

                                            // Device information before opening
                                            for (BGAPI2::DeviceList::iterator devIterator = pDeviceList->begin();
                                                devIterator != pDeviceList->end();
                                                devIterator++) {
                                                BGAPI2::Device* const pDevice = *devIterator;
                                                *log << "  5.2.3   Device DeviceID:        "
                                                    << pDevice->GetID() << std::endl;
                                                *log << "          Device Model:           "
                                                    << pDevice->GetModel() << std::endl;
                                                *log << "          Device SerialNumber:    "
                                                    << pDevice->GetSerialNumber() << std::endl;
                                                *log << "          Device Vendor:          "
                                                    << pDevice->GetVendor() << std::endl;
                                                *log << "          Device TLType:          "
                                                    << pDevice->GetTLType() << std::endl;
                                                *log << "          Device AccessStatus:    "
                                                    << pDevice->GetAccessStatus() << std::endl;
                                                *log << "          Device UserID:          "
                                                    << pDevice->GetDisplayName() << std::endl << std::endl;
                                            }

                                            for (BGAPI2::DeviceList::iterator devIterator = pDeviceList->begin();
                                                devIterator != pDeviceList->end();
                                                devIterator++) {
                                                try {
                                                    BGAPI2::Device* const pDevice = *devIterator;
                                                    pMatch->pDevice = pDevice;

                                                    GetDeviceInfo(log, pDevice, true);

                                                    if ((pDeviceFilter == NULL) || (pDeviceFilter(pDevice) == true)) {
                                                        return returncode;
                                                    }

                                                    *log << "        Close device (skipped) "
                                                        << std::endl << std::endl;
                                                    pDevice->Close();
                                                    pMatch->pDevice = NULL;
                                                }
                                                catch (BGAPI2::Exceptions::ResourceInUseException& ex) {
                                                    returncode = (returncode == 0) ? 1 : returncode;
                                                    *log << " Device  " << devIterator->GetID() << " already opened "
                                                        << std::endl;
                                                    *log << " ResourceInUseException: " << ex.GetErrorDescription()
                                                        << std::endl;
                                                }
                                                catch (BGAPI2::Exceptions::AccessDeniedException& ex) {
                                                    returncode = (returncode == 0) ? 1 : returncode;
                                                    *log << " Device  " << devIterator->GetID() << " already opened "
                                                        << std::endl;
                                                    *log << " AccessDeniedException " << ex.GetErrorDescription()
                                                        << std::endl;
                                                }
                                            }
                                        }
                                        catch (BGAPI2::Exceptions::IException& ex) {
                                            returncode = (returncode == 0) ? 1 : returncode;
                                            *log << "ExceptionType:    " << ex.GetType() << std::endl;
                                            *log << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                                            *log << "in function:      " << ex.GetFunctionName() << std::endl;
                                        }

                                        pReason = "no camera match";
                                    }
                                }

                                *log << "5.1.13   Close interface (" << pReason << ") " << std::endl << std::endl;
                                pInterface->Close();
                                pMatch->pInterface = NULL;
                            }
                            catch (BGAPI2::Exceptions::ResourceInUseException& ex) {
                                returncode = (returncode == 0) ? 1 : returncode;
                                *log << " Interface " << ifIterator->GetID() << " already opened " << std::endl;
                                *log << " ResourceInUseException: " << ex.GetErrorDescription() << std::endl;
                            }
                        }
                    }
                    catch (BGAPI2::Exceptions::IException& ex) {
                        returncode = (returncode == 0) ? 1 : returncode;
                        *log << "ExceptionType:    " << ex.GetType() << std::endl;
                        *log << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
                        *log << "in function:      " << ex.GetFunctionName() << std::endl;
                    }
                    pCloseSystemReason = "no camera match";
                }

                *log << "        Close system (" << pCloseSystemReason << ") " << std::endl << std::endl;
                pSystem->Close();
                pMatch->pSystem = NULL;
            }
            catch (BGAPI2::Exceptions::ResourceInUseException& ex) {
                returncode = (returncode == 0) ? 1 : returncode;
                *log << " System " << sysIterator->GetID() << " already opened " << std::endl;
                *log << " ResourceInUseException: " << ex.GetErrorDescription() << std::endl;
            }
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        *log << "ExceptionType:    " << ex.GetType() << std::endl;
        *log << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        *log << "in function:      " << ex.GetFunctionName() << std::endl;
    }
    return returncode;
}

// Helper to Display various information of the camera
void GetDeviceInfo(std::ostream* log, BGAPI2::Device* const pDevice, const bool bOpen) {
    *log << "5.1.7   Open device " << std::endl;
    *log << "          Device DeviceID:        " << pDevice->GetID() << std::endl;
    *log << "          Device Model:           " << pDevice->GetModel() << std::endl;
    *log << "          Device SerialNumber:    " << pDevice->GetSerialNumber() << std::endl;
    *log << "          Device Vendor:          " << pDevice->GetVendor() << std::endl;
    *log << "          Device TLType:          " << pDevice->GetTLType() << std::endl;
    *log << "          Device AccessStatus:    " << pDevice->GetAccessStatus() << std::endl;
    *log << "          Device UserID:          " << pDevice->GetDisplayName() << std::endl << std::endl;

    if (bOpen)
        pDevice->Open();

    *log << "        Opened device - RemoteNodeList Information " << std::endl;
    *log << "          Device AccessStatus:    " << pDevice->GetAccessStatus() << std::endl;

    BGAPI2::NodeMap* const pRemoteNodeList = pDevice->GetRemoteNodeList();
    // Serial number
    if (pRemoteNodeList->GetNodePresent("DeviceSerialNumber")) {
        *log << "          DeviceSerialNumber:     "
            << pRemoteNodeList->GetNode("DeviceSerialNumber")->GetValue() << std::endl;
    } else if (pRemoteNodeList->GetNodePresent("DeviceID")) {
        *log << "          DeviceID (SN):          "
            << pRemoteNodeList->GetNode("DeviceID")->GetValue() << std::endl;
    } else {
        *log << "          SerialNumber:           Not Available " << std::endl;
    }

    // Display DeviceManufacturerInfo
    if (pRemoteNodeList->GetNodePresent("DeviceManufacturerInfo")) {
        *log << "          DeviceManufacturerInfo: "
            << pRemoteNodeList->GetNode("DeviceManufacturerInfo")->GetValue() << std::endl;
    }

    // Display DeviceFirmwareVersion or DeviceVersion
    if (pRemoteNodeList->GetNodePresent("DeviceFirmwareVersion")) {
        *log << "          DeviceFirmwareVersion:  "
            << pRemoteNodeList->GetNode("DeviceFirmwareVersion")->GetValue() << std::endl;
    } else if (pRemoteNodeList->GetNodePresent("DeviceVersion")) {
        *log << "          DeviceVersion:          "
            << pRemoteNodeList->GetNode("DeviceVersion")->GetValue() << std::endl;
    } else {
        *log << "          DeviceVersion:          Not Available " << std::endl;
    }

    if (pDevice->GetTLType() == "GEV") {
        *log << "          GevCCP:                 "
            << pRemoteNodeList->GetNode("GevCCP")->GetValue() << std::endl;
        *log << "          GevCurrentIPAddress:    "
            << pRemoteNodeList->GetNode("GevCurrentIPAddress")->GetValue() << std::endl;
        *log << "          GevCurrentSubnetMask:   "
            << pRemoteNodeList->GetNode("GevCurrentSubnetMask")->GetValue() << std::endl;
    }

    *log << std::endl;
}

// Release all allocated resources
int ReleaseAllResources(BGAPI2::System* pSystem,
    BGAPI2::Interface* pInterface,
    BGAPI2::Device* pDevice,
    BGAPI2::DataStream* pDataStream,
    std::map<BGAPI2::Polarizer::Formats, BGAPI2::Image*>* requestedComponents) {
    try {
        if (pDataStream) {
            pDataStream->Close();
        }
        if (pDevice) {
            pDevice->Close();
        }
        if (pInterface) {
            pInterface->Close();
        }
        if (pSystem) {
            pSystem->Close();
        }
        BGAPI2::SystemList::ReleaseInstance();

        for (std::map<BGAPI2::Polarizer::Formats, BGAPI2::Image*>::iterator it = requestedComponents->begin();
            it != requestedComponents->end(); it++) {
            if (it->second != NULL) {
                it->second->Release();
                it->second = NULL;
            }
        }
        requestedComponents->clear();
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        return 1;
    }
    return 0;
}
