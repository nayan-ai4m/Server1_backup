/* Copyright 2019-2020 Baumer Optronic */
/*
    The example describes all how to use the provided Baumer GAPI API functionality to configure
    the camera and automatic brightness adjustment.
*/

#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

struct DeviceMatch {
    BGAPI2::System* pSystem;
    BGAPI2::Interface* pInterface;
    BGAPI2::Device* pDevice;
};

// connect to the first polarisation camera found on the system
static int GetFirstDevice(DeviceMatch* const pMatch,
    bool(*pSystemFilter)(BGAPI2::System* pSystem),
    bool(*pInterfaceFilter)(BGAPI2::Interface* pInterface),
    bool(*pDeviceFilter)(BGAPI2::Device* pDevice),
    std::ostream* log);

// Helper to Display various information of the camera
static void GetDeviceInfo(std::ostream* log, BGAPI2::Device* const pDevice, const bool bOpen);

// Release all allocated resources
static int ReleaseAllResources(BGAPI2::System* pSystem,
    BGAPI2::Interface* pInterface,
    BGAPI2::Device* pDevice,
    BGAPI2::DataStream* pDataStream);

// Helper to filter found cameras devices and select only camera with chunk support for this example
static bool DeviceWithChunkFilter(BGAPI2::Device* const pDevice);

// check the payload type: with JPEG mode the SOFTWARE mode isn't possible for the AutoBrightness component
static bool CheckCorrectPayloadType(BGAPI2::Device *const pDevice);
  
// Helper to convert BGAPI2::BrightnessAuto::AutoAlgorithm to string
static std::string GetBrightnessAutoAlgorithm(BGAPI2::BrightnessAuto::AutoAlgorithm algorithm);

// Helper to convert BGAPI2::BrightnessAuto::ControlFeature to string
static std::string GetBrightnessAutoControlFeature(BGAPI2::BrightnessAuto::ControlFeature control_feature);

// Helper to convert BGAPI2::BrightnessAuto::BrightnessAutoState to string
static std::string GetBrightnessAutoState(BGAPI2::BrightnessAuto::BrightnessAutoState state);

// Helper to Display BGAPI2::BrightnessAuto current settings
static void DisplayBrightnessAutoSettings(BGAPI2::BrightnessAuto* brightness_auto);

int main(int /*argc*/, char* /*argv*/[]) {
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

    int returncode = 0;

    bool bBufferedLog = true;


    std::cout << std::endl;
    std::cout << "#####################################################" << std::endl;
    std::cout << "# PROGRAMMER'S GUIDE Example 021_BrightnessAuto.cpp #" << std::endl;
    std::cout << "#####################################################" << std::endl;
    std::cout << std::endl << std::endl;

    // Find and open the first polarisation camera
    DeviceMatch match = { NULL, NULL, NULL };
    std::stringstream bufferedLog;

    const int code = GetFirstDevice(
        &match,
        NULL,
        NULL,
        DeviceWithChunkFilter,
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
            std::cout << " No supported device found " << sDataStreamID << std::endl;
        }

        std::cout << std::endl << "End" << std::endl << "Input any number to close the program: ";
        int endKey = 0;
        std::cin >> endKey;

        ReleaseAllResources(pSystem, pInterface, pDevice, pDataStream);
        return returncode;
    }

    GetDeviceInfo(&std::cout, pDevice, false);

    std::cout << "DEVICE PARAMETER SETUP" << std::endl;
    std::cout << "######################" << std::endl << std::endl;

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

    if (returncode) {
        ReleaseAllResources(pSystem, pInterface, pDevice, pDataStream);
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
        std::cout << std::endl << "End" << std::endl << "Input any number to close the program: ";
        int endKey = 0;
        std::cin >> endKey;
        ReleaseAllResources(pSystem, pInterface, pDevice, pDataStream);
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

    BGAPI2::Node* pExposureTime = NULL;
    BGAPI2::Node* pGain = NULL;
    try {
        BGAPI2::NodeMap* pRemoteNodeList = pDevice->GetRemoteNodeList();
        pExposureTime = pRemoteNodeList->GetNode("ExposureTime");

        if (pRemoteNodeList->GetNodePresent("Gain")) {
            BGAPI2::Node* pGainSelector = pRemoteNodeList->GetNode("GainSelector");
            if ((pGainSelector->GetEnumNodeList()->GetNodePresent("All")) &&
                (pGainSelector->GetEnumNodeList()->GetNode("All")->GetAvailable())) {
                pGainSelector->SetValue("All");
                pGain = pRemoteNodeList->GetNode("Gain");
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
    std::cout << "CONFIGURE BRIGHTNESSAUTO" << std::endl;
    std::cout << "########################" << std::endl << std::endl;

    try {
        std::cout << "  Limit exposure max to 500ms" << std::endl;
        pDevice->brightness_auto.SetExposureMaxValue(500000);

        std::cout << "  Start BrightnessAuto ONCE mode" << std::endl;
        pDevice->brightness_auto.SetMode(BGAPI2::BrightnessAuto::ONCE);
        std::cout << std::endl;

        DisplayBrightnessAutoSettings(&pDevice->brightness_auto);
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    if (returncode == 0) {
        // Capture 20 images
        int capture_count = 20;
        std::cout << std::endl;
        std::cout << "CAPTURE " << std::setw(2) << capture_count << " IMAGES BY IMAGE POLLING" << std::endl;
        std::cout << "##################################" << std::endl << std::endl;

        BGAPI2::BrightnessAuto::BrightnessAutoState state = BGAPI2::BrightnessAuto::DIDNOT_RUN;
        // Check BrightnessAuto state every cycle (optional)
        int check_state_cycle = 1;
        try {
            for (int i = 0; i < capture_count; i++) {
                BGAPI2::Buffer* pBufferFilled = pDataStream->GetFilledBuffer(2000);  // timeout 2000 msec
                std::cout << std::setw(4) << i << " - Exposure:"
                    << std::setw(9) << pExposureTime->GetDouble();
                if (pGain) {
                    std::cout << ", Gain:" << std::setw(5) << pGain->GetDouble();
                }
                if ((check_state_cycle > 0) && ((i % check_state_cycle) == 0)) {
                    state = pDevice->brightness_auto.GetState();
                    std::cout << ", " << GetBrightnessAutoState(state);
                    if (state != BGAPI2::BrightnessAuto::RUNNING_ONCE) {
                        check_state_cycle = 0;
                    }
                }
                std::cout << std::endl;
                if (pBufferFilled == NULL) {
                    std::cout << "Error: Buffer Timeout after 2000 msec" << std::endl;
                } else {
                    // Queue buffer again
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

        // Stop BrightnessAuto and control end state
        std::cout << "BrightnessAuto" << std::endl;
        std::cout << "  Stop" << std::endl;
        pDevice->brightness_auto.SetOff();

        state = pDevice->brightness_auto.GetState();
        std::cout << "  current state: " << GetBrightnessAutoState(state) << std::endl;
        if ((state != BGAPI2::BrightnessAuto::STOPPED_REACHED) && (state != BGAPI2::BrightnessAuto::STOPPED_NOT_REACHED)) {
            returncode = (returncode == 0) ? 1 : returncode;
            std::cout << "Error: unexpected BrightnessAuto state" << std::endl;
        }
    }
    std::cout << std::endl;

    std::cout << "CONFIGURE BRIGHTNESSAUTO" << std::endl;
    std::cout << "########################" << std::endl << std::endl;

    try {
        std::cout << "default: " << std::endl;
        std::cout << "  Algorithm: "
            << GetBrightnessAutoAlgorithm(pDevice->brightness_auto.GetAlgorithm()) << std::endl;
        std::cout << "  ControlFeature: "
            << GetBrightnessAutoControlFeature(pDevice->brightness_auto.GetControlFeature()) << std::endl;
        std::cout << std::endl;

        std::cout << "  Reset all parameters to default" << std::endl;
        pDevice->brightness_auto.Default();

        std::cout << "  Set nominal value to 75%" << std::endl;
        pDevice->brightness_auto.SetNominalValue(75.0);

        if (CheckCorrectPayloadType(pDevice)) {
            std::cout << "  Start CONTINUOUS mode with EXPOSURE_ONLY and SOFTWARE algorithm"
                      << std::endl;
            pDevice->brightness_auto.SetMode(
                BGAPI2::BrightnessAuto::CONTINUOUS,
                BGAPI2::BrightnessAuto::EXPOSURE_ONLY,
                BGAPI2::BrightnessAuto::SOFTWARE);
        } else {
            std::cout << "  Start CONTINUOUS mode with EXPOSURE_ONLY and HARDWARE algorithm,"
                      " because camera is in JPEG mode" << std::endl;
            pDevice->brightness_auto.SetMode(
                BGAPI2::BrightnessAuto::CONTINUOUS,
                BGAPI2::BrightnessAuto::EXPOSURE_ONLY,
                BGAPI2::BrightnessAuto::HARDWARE);
        }
        std::cout << std::endl;

        DisplayBrightnessAutoSettings(&pDevice->brightness_auto);
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    std::cout << std::endl;

    if (returncode == 0) {
      // Capture 30 images
      int capture_count = 30;
      std::cout << "CAPTURE " << std::setw(2) << capture_count
                << " IMAGES BY IMAGE POLLING" << std::endl;
      std::cout << "##################################" << std::endl
                << std::endl;

      BGAPI2::BrightnessAuto::BrightnessAutoState state = BGAPI2::BrightnessAuto::DIDNOT_RUN;
      // Check BrightnessAuto state every 5 cycles (optional)
      int check_state_cycle = 5;
      try {
        for (int i = 0; i < capture_count; i++) {
          bool check_state =
              (check_state_cycle > 0) && ((i % check_state_cycle) == 0);
          BGAPI2::Buffer *pBufferFilled =
              pDataStream->GetFilledBuffer(2000); // timeout 2000 msec
          std::cout << std::setw(4) << i << " - Exposure:" << std::setw(9)
                    << pExposureTime->GetDouble();
          if (pGain) {
            std::cout << ", Gain:" << std::setw(5) << pGain->GetDouble();
          }
          if (check_state) {
            state = pDevice->brightness_auto.GetState();
            std::cout << ", " << GetBrightnessAutoState(state);
          }
          std::cout << std::endl;
          if (check_state) {
            if (state != BGAPI2::BrightnessAuto::RUNNING_CONTINUOUS) {
              check_state_cycle = 0;
              returncode = (returncode == 0) ? 1 : returncode;
              std::cout << "Error: unexpected BrightnessAuto state"
                        << std::endl;
            }
          }
          if (pBufferFilled == NULL) {
            std::cout << "Error: Buffer Timeout after 2000 msec" << std::endl;
          } else {
            // Queue buffer again
            pBufferFilled->QueueBuffer();
          }
        }
      } catch (BGAPI2::Exceptions::IException &ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription()
                  << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
      }
      std::cout << " " << std::endl;

      // Stop BrightnessAuto and Ccontrol end state
      std::cout << "BrightnessAuto" << std::endl;

      std::cout << "  Stop" << std::endl;
      pDevice->brightness_auto.SetOff();

      state = pDevice->brightness_auto.GetState();
      std::cout << "  Current state: " << GetBrightnessAutoState(state)
                << std::endl;
      if ((state != BGAPI2::BrightnessAuto::STOPPED_REACHED) &&
          (state != BGAPI2::BrightnessAuto::STOPPED_NOT_REACHED)) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "Error: unexpected BrightnessAuto state" << std::endl;
      }
      std::cout << std::endl;

      std::cout << "CAMERA STOP" << std::endl;
      std::cout << "###########" << std::endl << std::endl;
    }
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

        ReleaseAllResources(pSystem, pInterface, pDevice, pDataStream);
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
    }

    std::cout << std::endl;
    std::cout << "End" << std::endl << std::endl;

    std::cout << "Input any number to close the program: ";
    int endKey = 0;
    std::cin >> endKey;
    return returncode;
}

std::string GetBrightnessAutoState(BGAPI2::BrightnessAuto::BrightnessAutoState state) {
    switch (state) {
        case BGAPI2::BrightnessAuto::DIDNOT_RUN:          return "DIDNOT_RUN";
        case BGAPI2::BrightnessAuto::RUNNING_ONCE:        return "RUNNING_ONCE";
        case BGAPI2::BrightnessAuto::RUNNING_CONTINUOUS:  return "RUNNING_CONTINUOUS";
        case BGAPI2::BrightnessAuto::STOPPED_REACHED:     return "STOPPED_REACHED";
        case BGAPI2::BrightnessAuto::STOPPED_NOT_REACHED: return "STOPPED_NOT_REACHED";
        case BGAPI2::BrightnessAuto::STOPPED_ERROR:       return "STOPPED_ERROR";
        default:                                          return "Unknow";
    }
}

std::string GetBrightnessAutoAlgorithm(BGAPI2::BrightnessAuto::AutoAlgorithm algorithm) {
    switch (algorithm) {
        case BGAPI2::BrightnessAuto::SOFTWARE: return "SOFTWARE";
        case BGAPI2::BrightnessAuto::HARDWARE: return "HARDWARE";
        default:                               return "Unknow";
    }
}

std::string GetBrightnessAutoControlFeature(BGAPI2::BrightnessAuto::ControlFeature control_feature) {
    switch (control_feature) {
        case BGAPI2::BrightnessAuto::EXPOSURE_ONLY: return "EXPOSURE_ONLY";
        case BGAPI2::BrightnessAuto::GAIN_ONLY:     return "GAIN_ONLY";
        case BGAPI2::BrightnessAuto::EXPOSURE_PRIO: return "EXPOSURE_PRIO";
        case BGAPI2::BrightnessAuto::GAIN_PRIO:     return "GAIN_PRIO";
        default:                                    return "Unknow";
    }
}

void DisplayBrightnessAutoSettings(BGAPI2::BrightnessAuto* brightness_auto) {
    std::cout << std::fixed << std::setprecision(1);

    std::cout << "  Algorithm:      "
        << GetBrightnessAutoAlgorithm(brightness_auto->GetAlgorithm()) << std::endl;
    BGAPI2::BrightnessAuto::ControlFeature control_feature = brightness_auto->GetControlFeature();
        std::cout << "  ControlFeature: " << GetBrightnessAutoControlFeature(control_feature) << std::endl;

    if (brightness_auto->IsExposureEnabled()) {
        std::cout << "  Exposure min:            "
            << std::setw(9) << brightness_auto->GetExposureMinValue() << std::endl;
        std::cout << "  Exposure max:            "
            << std::setw(9) << brightness_auto->GetExposureMaxValue() << std::endl;
    }
    if (brightness_auto->IsGainEnabled()) {
        std::cout << "  Gain min:                "
            << std::setw(9) << brightness_auto->GetGainMinValue() << std::endl;
        std::cout << "  Gain max:                "
            << std::setw(9) << brightness_auto->GetGainMaxValue() << std::endl;
    }
    std::cout << "  Nominal value:           "
        << std::setw(9) << brightness_auto->GetNominalValue() << "%" << std::endl;
}

// Helper to filter found cameras devices and select only camera with chunk support for this example
bool DeviceWithChunkFilter(BGAPI2::Device* const pDevice) {
    if (pDevice->GetRemoteNodeList()->GetNodePresent("ChunkSelector")) {
        return true;
    }
    return false;
}

// check the payload type: with JPEG mode the SOFTWARE mode isn't possible for the AutoBrightness component
bool CheckCorrectPayloadType(BGAPI2::Device *const pDevice) {
    if (pDevice->GetRemoteNodeList()->GetNodePresent("BrightnessAutoPriority")  // hardware controlling is available
        && pDevice->GetRemoteNodeList()->GetNodePresent("ImageCompressionMode")) { // camera has possibility of JPEGs
       return pDevice->GetRemoteNode("ImageCompressionMode")->GetString() == "Off";  // camera isn't in JPEG mode
    }
    return true;
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
    BGAPI2::DataStream* pDataStream) {
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
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        std::cout << "ExceptionType:    " << ex.GetType() << std::endl;
        std::cout << "ErrorDescription: " << ex.GetErrorDescription() << std::endl;
        std::cout << "in function:      " << ex.GetFunctionName() << std::endl;
        return 1;
    }
    return 0;
}
