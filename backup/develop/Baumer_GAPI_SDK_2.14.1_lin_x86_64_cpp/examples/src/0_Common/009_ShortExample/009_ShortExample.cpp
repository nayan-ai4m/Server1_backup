/* Copyright 2019-2020 Baumer Optronic */
#include <stdio.h>
#include <iostream>
#include "bgapi2_genicam/bgapi2_genicam.hpp"

int main() {
    int returncode = 0;
    int camfound = 0;
    try {
        BGAPI2::SystemList *systemList = BGAPI2::SystemList::GetInstance();
        systemList->Refresh();

        for (BGAPI2::SystemList::iterator it_s = systemList->begin();
            it_s != systemList->end() && camfound == 0;
            it_s++) {
            BGAPI2::System *pSystem = *it_s;  // gige, usb3, ..
            pSystem->Open();

            BGAPI2::InterfaceList *interfaceList = pSystem->GetInterfaces();
            interfaceList->Refresh(100);
            for (BGAPI2::InterfaceList::iterator it_i = interfaceList->begin();
                it_i != interfaceList->end() && camfound == 0;
                it_i++) {
                BGAPI2::Interface *pInterface = *it_i;
                pInterface->Open();

                BGAPI2::DeviceList *deviceList = pInterface->GetDevices();
                deviceList->Refresh(100);
                if (deviceList->size() > 0) {
                    BGAPI2::Device *pDevice = *(deviceList->begin());
                    pDevice->Open();
                    std::cout << pDevice->GetModel() << "(" << pDevice->GetSerialNumber() << ")" << std::endl;
                    pDevice->GetRemoteNode("TriggerMode")->SetString("Off");
                    BGAPI2::String sExposureNodeName = "";
                    if (pDevice->GetRemoteNodeList()->GetNodePresent("ExposureTime")) {
                        sExposureNodeName = "ExposureTime";
                    } else if (pDevice->GetRemoteNodeList()->GetNodePresent("ExposureTimeAbs")) {
                        sExposureNodeName = "ExposureTimeAbs";
                    }
                    pDevice->GetRemoteNode(sExposureNodeName)->SetDouble(10000.0);

                    BGAPI2::DataStreamList *datastreamList = pDevice->GetDataStreams();
                    datastreamList->Refresh();
                    BGAPI2::DataStream *pDataStream = (*datastreamList)[0];
                    pDataStream->Open();

                    BGAPI2::BufferList *bufferList = pDataStream->GetBufferList();
                    BGAPI2::Buffer * pBuffer = NULL;
                    for (int i = 0; i < 4; i++) {
                        pBuffer = new BGAPI2::Buffer();
                        bufferList->Add(pBuffer);
                        pBuffer->QueueBuffer();
                    }

                    pDataStream->StartAcquisitionContinuous();
                    pDevice->GetRemoteNode("AcquisitionStart")->Execute();

                    BGAPI2::Buffer * pBufferFilled = NULL;
                    for (int i = 0; i < 12; i++) {
                        pBufferFilled = pDataStream->GetFilledBuffer(1000);
                        if (pBufferFilled == NULL) {
                            std::cout << "Error: Buffer Timeout after 1000 msec" << std::endl;
                        } else if (pBufferFilled->GetIsIncomplete() == true) {
                            std::cout << "Error: Image is incomplete" << std::endl;
                            pBufferFilled->QueueBuffer();
                        } else {
                            std::cout << " Image " << pBufferFilled->GetFrameID() << " received. " << std::endl;
                            pBufferFilled->QueueBuffer();
                        }
                    }
                    if (pDevice->GetRemoteNodeList()->GetNodePresent("AcquisitionAbort")) {
                        pDevice->GetRemoteNode("AcquisitionAbort")->Execute();
                    }
                    pDevice->GetRemoteNode("AcquisitionStop")->Execute();
                    pDataStream->StopAcquisition();

                    bufferList->DiscardAllBuffers();
                    while (bufferList->size() > 0) {
                        pBuffer = *(bufferList->begin());
                        bufferList->RevokeBuffer(pBuffer);
                        delete pBuffer;
                    }
                    pDataStream->Close();
                    pDevice->Close();
                    camfound = 1;
                }
                pInterface->Close();
            }
            pSystem->Close();
        }
        if (camfound == 0) {
            std::cout << "no camera found on any system or interface." << std::endl;
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        returncode = (returncode == 0) ? 1 : returncode;
        std::cout << "Error in function: " << ex.GetFunctionName() << std::endl << "Error description: "
            << ex.GetErrorDescription() << std::endl << std::endl;
    }
    BGAPI2::SystemList::ReleaseInstance();
    std::cout << "Input any number to close the program:";
    int endKey = 0;
    std::cin >> endKey;
    return returncode;
}
